# Architecture Guide

System design and implementation details for HPDS Ingest Service.

## System Architecture

```
┌─────────────────────────────────────────────────────────┐
│                  Ingest Service                         │
│  ┌──────────────────────┐  ┌───────────────────────┐    │
│  │ ParquetObservation   │  │ CsvObservation        │    │
│  │ Producer             │  │ Producer              │    │
│  │ (schema projection,  │  │ (streaming parse)     │    │
│  │  row-group stream)   │  │                       │    │
│  └──────────┬───────────┘  └───────────┬───────────┘    │
│             │                          │                │
│             └───────────┬──────────────┘                │
│                         ▼                               │
│            ┌─────────────────────────┐                  │
│            │  ObservationRow         │                  │
│            │  (canonical model)      │                  │
│            └──────────┬──────────────┘                  │
│                       ▼                                 │
│            ┌─────────────────────────┐                  │
│            │  HPDSWriterAdapter      │                  │
│            └──────────┬──────────────┘                  │
└───────────────────────┼─────────────────────────────────┘
                        ▼
          ┌─────────────────────────────┐
          │  SpoolingLoadingStore       │
          │  ┌──────────────────────┐   │
          │  │ In-memory cache      │   │
          │  │ (LRU, size=50,000)   │   │
          │  └──────────┬───────────┘   │
          │             │ eviction      │
          │             ▼               │
          │  ┌──────────────────────┐   │
          │  │ Per-concept spool    │   │
          │  │ (gzipped partials)   │   │
          │  └──────────────────────┘   │
          └──────────┬──────────────────┘
                     │ finalize
                     ▼
          ┌─────────────────────────────┐
          │  allObservationsStore       │
          │  columnMeta.javabin         │
          └─────────────────────────────┘
```

## Module Structure

```
pic-sure-hpds/
├── hpds-writer-adapter/          # Adapter layer for HPDS stores
│   ├── ObservationRow            # Canonical internal record
│   ├── SpoolingLoadingStore      # Enhanced store with spool-and-finalize
│   └── HPDSWriterAdapter         # Facade for producers
│
└── ingest-service/               # Spring Boot orchestration
    ├── producer/
    │   ├── ParquetObservationProducer  # Parquet → ObservationRow
    │   └── CsvObservationProducer      # CSV → ObservationRow
    ├── failure/
    │   ├── FailureReason         # Stable enum for failure codes
    │   ├── FailureRecord         # JSONL record structure
    │   └── FailureSink           # Thread-safe JSONL writer
    ├── config/
    │   └── ParquetDatasetConfig  # Per-dataset mapping
    └── IngestServiceApplication  # Main orchestrator
```

## Component Details

### Observation Producers

**ParquetObservationProducer**
- Reads Parquet files using Apache Arrow Dataset API
- Zero-copy memory access via off-heap buffers
- Schema projection (only reads configured columns)
- Enforces per-file observation limits
- Converts rows to canonical `ObservationRow` model

**CsvObservationProducer**
- Streams legacy CSV files
- Parses patient ID, concept path, timestamp, value
- Compatible with original HPDS CSV format

### HPDSWriterAdapter

Facade layer providing unified interface for producers:

```java
public interface HPDSWriterAdapter {
    void addObservation(ObservationRow row);
    void saveStore();
}
```

Delegates to `SpoolingLoadingStore` for actual storage.

### SpoolingLoadingStore

Enhanced version of original `LoadingStore` that supports multiple flushes per concept.

**Key features:**
- LRU cache for in-memory PhenoCubes (configurable size)
- Automatic spool-to-disk on cache eviction
- Per-concept observation tracking (enforces limits)
- Parallel finalization with deterministic ordering

**Critical implementation details:**

1. **Total observation tracking**
   ```java
   private static class ConceptMetadata {
       AtomicLong totalObservationCount = new AtomicLong(0);
       AtomicLong observationCount = new AtomicLong(0);  // Current spool
   }
   ```

2. **Rejection logic** (v3.1.0 bug fix)
   ```java
   // Check total first - REJECT if limit reached
   if (meta.totalObservationCount.get() >= maxObservationsPerConcept) {
       conceptRejections.computeIfAbsent(conceptPath, k -> new AtomicLong(0))
                        .incrementAndGet();
       return; // Don't add observation
   }
   ```

3. **Spool management** (memory only)
   ```java
   // Separate check for per-spool limit (cache eviction)
   if (meta.observationCount.get() >= maxObservationsPerConcept) {
       spoolPartial(conceptPath, cube);
       cache.invalidate(conceptPath);
       meta.observationCount.set(0);  // Reset for next spool
   }
   ```

## Data Flow

### Ingestion Phase

```
1. Load dataset configs (JSONL)
2. For each dataset:
   a. Scan parquet directory for participant files
   b. Submit files to bounded virtual thread pool
3. For each file (parallel):
   a. Open with Arrow Dataset API
   b. Project schema (only configured columns)
   c. For each row:
      - Parse participant ID, timestamp, values
      - Generate ObservationRow per variable
      - Check per-file limit (stop if exceeded)
      - Pass to HPDSWriterAdapter
4. HPDSWriterAdapter → SpoolingLoadingStore:
   a. Check per-concept total limit (reject if exceeded)
   b. Add to in-memory PhenoCube
   c. On cache eviction → spool to disk
5. Await all files complete
```

### Finalization Phase

```
1. Flush remaining cache → spool partials
2. Build concept list (deterministic sort)
3. For each concept (parallel, bounded concurrency):
   a. Load all spool files for concept
   b. Merge KeyAndValue entries
   c. Sort by key (patientNum, timestamp)
   d. Build category maps (for categorical concepts)
   e. Compute min/max (for numeric concepts)
   f. Write ONCE to allObservationsStore
   g. Write ONCE to columnMeta
4. Delete spool directory
5. Write concept_rejections.jsonl
```

## SpoolingLoadingStore vs Original LoadingStore

### Problem with Original LoadingStore

The original implementation writes exactly ONE entry per concept:
- On first flush: writes PhenoCube at offset X
- On second flush: **overwrites** at offset X
- Result: data loss if concept appears in multiple batches

This forced requirement: **global ordering by concept path**

### Solution: Spool-and-Finalize

**Key innovation**: Allow multiple partial writes per concept, merge at end

**During ingestion:**
- Partial PhenoCubes written to spool files: `spool/{conceptPath}.partial.{n}.gz`
- No coordination required between sources
- Memory bounded by cache size (not concept cardinality)

**At finalization:**
- All partials for concept merged into single PhenoCube
- Written ONCE to allObservationsStore
- Exactly one ColumnMeta entry

**Result:**
- Sources can be interleaved (no global sorting required)
- Parquet and CSV can be processed simultaneously
- Memory usage bounded and predictable

## Concurrency Model

### Virtual Threads (Project Loom)

**File processing pool:**
```java
ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
executor = bounded(executor, maxThreads);
```

Bounded virtual threads provide:
- Low overhead (millions possible)
- Efficient I/O wait (no thread blocking)
- Back-pressure via semaphore

**Configuration:**
- i7i.24xlarge: 72 threads (0.75 × vCPU count)
- i7i.12xlarge: 36 threads
- i7i.8xlarge: 24 threads

### Thread Safety

**SpoolingLoadingStore synchronization:**
- `ConcurrentHashMap` for concept metadata
- `AtomicLong` for observation counters
- Concept-level locking during finalization
- Cache eviction synchronized per concept

**FailureSink:**
- Thread-safe JSONL writer
- Buffered writes with explicit flush

## Memory Management

### Heap Memory

**Primary consumers:**
1. **In-memory PhenoCubes** (LRU cache)
   - Size: 50,000 concepts (large instances)
   - Per-concept: ~190MB at 100M observations
   - Total: ~9.5GB at capacity

2. **Spring context** (~500MB)

3. **Observation batches** (~2GB with 72 threads)

**Configured via:**
```bash
-Xms<heap>g -Xmx<heap>g
```

### Direct Memory (Off-Heap)

**Used by Apache Arrow:**
- File buffers during parquet reading
- Per file: ~100-200MB depending on schema width
- With 72 threads: 72 × 200MB = ~14GB

**Configured via:**
```bash
-XX:MaxDirectMemorySize=<direct>g
```

### Disk (NVMe Spool)

**Spool files:**
- Location: `/mnt/nvme/spool` (i7i instances have local NVMe)
- Format: gzipped PhenoCube partials
- Size: ~50MB per partial (compressed)
- Cleanup: automatic on successful finalization

## Failure Handling

### Failure Types

**Ingestion failures:**
- Missing/invalid participant ID
- Missing/invalid timestamp
- Unparseable numeric value
- Schema mismatch

**System failures:**
- OutOfMemoryError → heap dump + exit
- Disk full → abort with error
- Parquet corruption → skip file, log failure

### Failure Tracking

**FailureSink** writes JSONL with full context:
```json
{
  "sourceType": "PARQUET",
  "dataset": "fitbit_activitylogs",
  "inputFile": "/path/to/file.parquet",
  "participantId": "12345",
  "reasonCode": "NUMERIC_PARSE_ERROR",
  "reasonDetail": "Value 'abc' is not a valid number"
}
```

**Data loss tracking** via `concept_rejections.jsonl`:
```json
{
  "conceptPath": "\\FitBit\\HeartRate\\",
  "observationsAccepted": 100000000,
  "observationsRejected": 25000000,
  "rejectionRate": 20.0
}
```

## Performance Characteristics

### Memory Bounds

| Component | Bound | Configuration |
|-----------|-------|---------------|
| Heap | Cache size × concept size | `ingest.store-cache-size` |
| Direct | Thread count × buffer size | `ingest.file-processing-threads` |
| Disk | Spool size (temp) | Automatic cleanup |

### Throughput

| Dataset | Size | Throughput (i7i.24xlarge) |
|---------|------|---------------------------|
| HealthKitV2Samples | 1.36B obs, 1,198 files | 3-5 minutes |
| FitbitIntradaycombined | 3,443 files | 30-45 minutes |
| Full 18 datasets | ~260GB parquet | 2-3 hours |

### Scalability

**Horizontal:** Not applicable (single-node)
**Vertical:** Linear scaling with vCPU count up to I/O saturation

**Bottlenecks:**
1. Disk I/O (NVMe spool during finalization)
2. Memory (cache size vs dataset cardinality)
3. CPU (compression/decompression)

## Next Steps

- **Configuration**: See [Configuration Guide](CONFIGURATION.md) for tuning parameters
- **Deployment**: See [Deployment Guide](DEPLOYMENT.md) for instance sizing and JVM tuning
- **Operations**: See [Operations Guide](OPERATIONS.md) for monitoring and troubleshooting

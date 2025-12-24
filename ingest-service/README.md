# HPDS Multi-Source Ingest Service

## Overview

This service ingests heterogeneous data sources (Parquet, CSV) directly into HPDS observation stores without materializing a unified `allConcepts.csv`. It uses an enhanced LoadingStore with spool-and-finalize semantics to handle multiple batches per concept.

### Key Features

- **No global concept grouping required** - observations from multiple sources can be interleaved
- **Bounded memory** - streams large Parquet files using schema projection
- **Failure tracking** - JSONL output for Splunk/Elastic consumption
- **Atomic finalization** - each concept written exactly once to allObservationsStore
- **Backward compatible** - no changes to HPDS readers or on-disk formats

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                  Ingest Service                         │
│  ┌──────────────────────┐  ┌───────────────────────┐   │
│  │ ParquetObservation   │  │ CsvObservation        │   │
│  │ Producer             │  │ Producer              │   │
│  │ (schema projection,  │  │ (streaming parse)     │   │
│  │  row-group stream)   │  │                       │   │
│  └──────────┬───────────┘  └───────────┬───────────┘   │
│             │                           │               │
│             └───────────┬───────────────┘               │
│                         ▼                               │
│            ┌─────────────────────────┐                  │
│            │  ObservationRow         │                  │
│            │  (canonical model)      │                  │
│            └──────────┬──────────────┘                  │
│                       ▼                                 │
│            ┌─────────────────────────┐                  │
│            │  HPDSWriterAdapter      │                  │
│            └──────────┬──────────────┘                  │
└───────────────────────┼──────────────────────────────────┘
                        ▼
          ┌─────────────────────────────┐
          │  SpoolingLoadingStore       │
          │  ┌──────────────────────┐   │
          │  │ In-memory cache      │   │
          │  │ (LRU, size=16)       │   │
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

## SpoolingLoadingStore Enhancement

### Problem with Original LoadingStore

The original `LoadingStore` writes exactly ONE serialized PhenoCube per conceptPath and stores a single `(offset, length)` entry in `columnMeta`. If the same conceptPath is flushed more than once from the cache, earlier data is **overwritten**. This forced global grouping by conceptPath.

### Solution: Spool-and-Finalize

**During ingestion:**
1. Observations are added to in-memory PhenoCubes (LRU cache)
2. On cache eviction, the partial cube is **spooled** to disk (gzipped, per-concept)
3. Multiple partials for the same concept are allowed

**At finalize time (saveStore):**
1. Flush cache (spools remaining concepts)
2. For each conceptPath (deterministic order):
   - Read all spooled partials
   - Merge KeyAndValue entries
   - Sort by key
   - Build category maps / compute min/max
   - Write exactly ONCE to allObservationsStore
   - Write exactly one ColumnMeta entry

**Result:** Global concept ordering is no longer required. Sources can be interleaved safely.

## Maven Build Commands

### Build only the new modules (fastest)

```bash
# From pic-sure-hpds/ root
mvn -pl :hpds-writer-adapter,:ingest-service -am clean install
```

### Build and package the service

```bash
mvn -pl :ingest-service -am clean package
```

### Run tests

```bash
mvn -pl :hpds-writer-adapter,:ingest-service -am test
```

### Build without running tests

```bash
mvn -pl :ingest-service -am clean package -DskipTests
```

## Running the Service

### Basic execution

```bash
java -jar ingest-service/target/ingest-service-3.0.0-SNAPSHOT.jar \
  --parquet.dir=/path/to/data \
  --parquet.config=/path/to/datasets.jsonl \
  --csv.dir=/path/to/legacy/csvs \
  --output.dir=/opt/local/hpds/ \
  --spool.dir=/opt/local/hpds/spool \
  --failure.file=/opt/local/hpds/failures.jsonl
```

### Parquet-only ingestion

```bash
java -jar ingest-service/target/ingest-service-3.0.0-SNAPSHOT.jar \
  --parquet.dir=/Users/thomas/IdeaProjects/local-parquet-poc-dhdr/nih-nhlbi-bdc-recover-adult-phs003463-v5-r1-c1/data/DHDR \
  --parquet.config=datasets.jsonl \
  --output.dir=/opt/local/hpds/
```

### CSV-only ingestion

```bash
java -jar ingest-service/target/ingest-service-3.0.0-SNAPSHOT.jar \
  --csv.dir=/path/to/split/csvs \
  --output.dir=/opt/local/hpds/
```

## Parquet Dataset Configuration

Create a JSONL file (one dataset per line):

```jsonl
{"datasetName":"fitbit_activitylogs","deviceName":"FitBit","participantIdColumn":"ParticipantIdentifier","timestampColumn":"Date","variableColumns":["Calories","Steps"],"variableLabels":["Calories","Steps"]}
{"datasetName":"fitbit_sleeplogs","deviceName":"FitBit","participantIdColumn":"ParticipantIdentifier","timestampColumn":"SleepDay","variableColumns":["TotalMinutesAsleep","TotalTimeInBed"],"variableLabels":["TotalMinutesAsleep","TotalTimeInBed"]}
{"datasetName":"garmin_dailies","deviceName":"Garmin","participantIdColumn":"ParticipantIdentifier","timestampColumn":"CalendarDate","variableColumns":["TotalSteps","TotalDistanceMeters"],"variableLabels":["TotalSteps","TotalDistanceMeters"]}
```

### Configuration fields

- `datasetName`: Subdirectory name under DHDR
- `deviceName`: Used in concept path (e.g., "FitBit", "Garmin")
- `participantIdColumn`: Column containing participant ID
- `timestampColumn`: Column containing timestamp, or "none" for relational child tables
- `variableColumns`: List of columns to ingest
- `variableLabels`: Human-readable labels (parallel to variableColumns)

## Concept Path Format

HPDS uses backslash-delimited concept paths with trailing delimiter:

```
\phs003463\RECOVER_Adult\DigitalHealthData\<device>\<variable>\
```

Example:
```
\phs003463\RECOVER_Adult\DigitalHealthData\FitBit\Calories\
\phs003463\RECOVER_Adult\DigitalHealthData\Garmin\TotalSteps\
```

## Failure Tracking

All failures are written to a JSONL file with full context:

```json
{
  "runId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "sourceType": "PARQUET",
  "dataset": "fitbit_activitylogs",
  "inputFile": "/path/to/file.parquet",
  "participantId": "12345",
  "patientNum": null,
  "conceptPath": null,
  "timestampRaw": "2024-01-15T10:30:00Z",
  "timestampParsed": null,
  "valueRaw": "abc",
  "numericParseError": "Cannot parse as double",
  "reasonCode": "NUMERIC_PARSE_ERROR",
  "reasonDetail": "Value 'abc' is not a valid number"
}
```

### Failure reason codes

- `MISSING_PATIENT_ID` - Patient ID column missing or null
- `INVALID_PATIENT_ID` - Patient ID not an integer
- `MISSING_CONCEPT_PATH` - Concept path missing or empty
- `INVALID_CONCEPT_PATH` - Concept path malformed
- `MISSING_TIMESTAMP_COLUMN` - Configured timestamp column not found
- `INVALID_TIMESTAMP` - Timestamp unparseable
- `MISSING_VALUE` - Both numeric and text values are null
- `DUPLICATE_VALUE` - Both numeric and text values are non-null
- `NUMERIC_PARSE_ERROR` - Numeric value unparseable
- `SCHEMA_MISMATCH` - Parquet schema does not match config
- `FILE_READ_ERROR` - Unable to read source file
- `DATASET_SKIPPED_NO_TIMESTAMP` - Dataset skipped (no timestamp configured)
- `UNKNOWN` - Unknown failure

## Validation & Testing Plan

### 1. Multi-batch same-concept ingestion

**Test:** Ingest observations for the same concept from multiple sources
**Validation:**
- Query HPDS for the concept
- Verify all observations are present (count matches expected)
- Verify no duplicates

### 2. DHDR + CSV interleaving

**Test:** Ingest Parquet datasets and CSV files in single run
**Validation:**
- Check allObservationsStore.javabin size
- Check columnMeta.javabin contains concepts from both sources
- Query spanning both source types

### 3. Backward compatibility

**Test:** Load stores created by new service with existing HPDS query service
**Validation:**
- Start existing HPDS service
- Run queries against new stores
- Verify results match expected

### 4. Memory bounds

**Test:** Ingest 260GB DHDR dataset
**Monitor:**
- JVM heap usage (should remain bounded)
- Spool directory growth
- No OutOfMemoryError

### 5. Failure tracking completeness

**Test:** Inject malformed data (missing IDs, invalid timestamps, unparseable numerics)
**Validation:**
- All failures appear in failures.jsonl
- Rollup counts match injected failures
- No silent drops

### 6. Spool cleanup

**Test:** Run full ingestion
**Validation:**
- Spool directory is empty after successful completion
- No .partial.*.gz files remain

## Output Artifacts

### allObservationsStore.javabin

Binary file containing encrypted, serialized PhenoCubes. Format unchanged from original HPDS.

### columnMeta.javabin

Gzipped serialized TreeMap containing:
- Concept metadata (name, type, category values, min/max, offsets)
- Set of all patient IDs

Format unchanged from original HPDS.

### columnMeta.csv

Human-readable CSV export of metadata (generated by LoadingStore).

### failures.jsonl

JSONL file with one failure record per line, plus rollup summary at end.

### ingest.log

Application log with progress, warnings, and errors.

## Performance Characteristics

### Memory usage

- **Bounded** by cache size (default: 16 concepts in memory)
- Spool files are compressed (gzip)
- Parquet reading uses schema projection (only needed columns)

### Disk I/O

- **Sequential writes** during spool phase
- **Sequential reads** during finalize phase
- **Single write** per concept to allObservationsStore

### Parallelism

- **Producer parallelism**: Multiple Parquet/CSV files can be processed concurrently (controlled by thread pool)
- **Writer serialization**: HPDSWriterAdapter.acceptBatch() is thread-safe; finalization is single-threaded

## Troubleshooting

### OutOfMemoryError during ingestion

- Reduce cache size: modify `cacheSize` parameter in HPDSWriterAdapter constructor
- Increase JVM heap: `java -Xmx16g -jar ingest-service.jar ...`

### Spool directory fills disk

- Check for runaway concept cardinality (millions of unique concepts)
- Increase cache size to reduce spool frequency
- Monitor spool file count: `ls -1 /opt/local/hpds/spool | wc -l`

### Parquet schema mismatch

- Verify dataset config matches actual Parquet schema
- Use `parquet-tools schema file.parquet` to inspect schema
- Check failures.jsonl for SCHEMA_MISMATCH entries

### Timestamp parsing failures

- HPDS expects ISO 8601 format: `2024-01-15T10:30:00Z`
- Check failures.jsonl for INVALID_TIMESTAMP entries
- Verify dataset config points to correct timestamp column

## Differences from Original LoadingStore

| Aspect | Original LoadingStore | SpoolingLoadingStore |
|--------|----------------------|---------------------|
| Concept batching | Requires global grouping | Allows interleaving |
| Memory usage | Unbounded (depends on concept count) | Bounded by cache size |
| Write semantics | Overwrites on re-flush | Accumulates via spool |
| Finalization | Inline during cache eviction | Deferred to saveStore() |
| On-disk format | Unchanged | Unchanged |
| Reader compatibility | N/A | Fully compatible |

## Future Enhancements

- [ ] External merge for very high cardinality concepts
- [ ] Parallel finalization (requires lock coordination)
- [ ] Resume from partial run (checkpoint/restart)
- [ ] Streaming metrics (Prometheus/Micrometer)
- [ ] Join support for relational child tables (inherit parent timestamp)
- [ ] Categorical threshold tuning (auto-detect vs force numeric)

# HPDS Multi-Source Ingest Service

## Overview

This service ingests heterogeneous data sources (Parquet, CSV) directly into HPDS observation stores without materializing a unified `allConcepts.csv`. It uses an enhanced LoadingStore with spool-and-finalize semantics to handle multiple batches per concept.

### Key Features

- **No global concept grouping required** - observations from multiple sources can be interleaved
- **Bounded memory** - streams large Parquet files using schema projection
- **Per-file observation limits** - prevents OOM on extremely large files (configurable, default: 1M observations/file)
- **Concept observation limits** - enforces hard limits per concept to prevent Java array overflow (configurable, default: 100M observations/concept)
- **Failure tracking** - JSONL output for Splunk/Elastic consumption
- **Data loss tracking** - reports observations rejected due to limits (`file_limits.jsonl`, `concept_rejections.jsonl`)
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
{"dataset":"dataset_fitbitintradaycombined","device":"FitBit","participantIdColumn":"ParticipantIdentifier","timestampColumn":"DateTime","conceptPathPrefix":["phs003463","RECOVER_Adult","DigitalHealthData","FitBit","Intradaycombined"],"variables":[{"column":"Type","label":"Type"},{"column":"DateTime","label":"DateTime"},{"column":"Value","label":"Value"}]}
{"dataset":"dataset_healthkitv2samples","device":"HealthKit","participantIdColumn":"ParticipantIdentifier","timestampColumn":"StartDate","conceptPathPrefix":["phs003463","RECOVER_Adult","DigitalHealthData","HealthKit","V2samples"],"variables":[{"column":"Date","label":"Date"},{"column":"Value","label":"Value"}]}
{"dataset":"enrolledparticipants","device":"Unknown","participantIdColumn":"ParticipantIdentifier","timestampColumn":"none","maxObservationsPerFile":null,"conceptPathPrefix":["phs003463","RECOVER_Adult","Demographics"],"variables":[{"column":"EnrollmentDate","label":"EnrollmentDate"}]}
```

### Configuration fields

**Required:**
- `dataset`: Dataset name (used as subdirectory name under parquet base directory)
- `device`: Device name used in concept path (e.g., "FitBit", "HealthKit")
- `participantIdColumn`: Column containing participant ID
- `timestampColumn`: Column containing timestamp, or "none" for relational child tables
- `conceptPathPrefix`: Array of path components for concept hierarchy
- `variables`: Array of variable configurations (see below)

**Optional:**
- `maxObservationsPerFile`: Per-file observation limit (default: 1,000,000)
  - Set to `null` for unlimited (use for small datasets only)
  - Prevents OOM on extremely large individual files
  - Example: `"maxObservationsPerFile": 500000` (500K observations per file)

### Variable configuration

Each variable has:
- `column`: Parquet column name (required)
- `label`: Human-readable label for concept path (required)
- `forceType`: Force type interpretation (optional)
  - `"NUMERIC"`: Parse as numeric, fail on non-numeric values
  - `"TEXT"`: Store as categorical text
  - `null` or omitted: Auto-detect based on value parsing

## Concept Path Format

HPDS uses backslash-delimited concept paths with trailing delimiter:

```
\phs003463\RECOVER_Adult\DigitalHealthData\<device>\<variable>\
```

Example:
```
\phs003463\RECOVER_Adult\DigitalHealthData\FitBit\Type\
\phs003463\RECOVER_Adult\DigitalHealthData\HealthKit\Value\
```

## Observation Limits & Data Loss Tracking

The ingest service implements a **two-tier limiting strategy** to prevent out-of-memory errors and Java array overflow while maintaining data quality:

### Tier 1: Per-File Observation Limit

**Purpose:** Prevents OOM when processing extremely large individual parquet files

**Default:** 1,000,000 observations per file

**How it works:**
1. Each parquet file represents one individual's data for a dataset
2. File processing stops when the limit is reached
3. Remaining rows in the file are skipped (not ingested)
4. Ensures deterministic, repeatable behavior (same rows always ingested)

**Configuration:**

Global default (applies to all datasets):
```properties
# application-ingest.properties
ingest.max-observations-per-file=1000000
```

Per-dataset override:
```jsonl
{"dataset":"dataset_fitbitintradaycombined","maxObservationsPerFile":500000,...}
```

**Example:**

File: `participant_12345_fitbitintradaycombined.parquet`
- Total rows: 10,000,000
- Variables: 13 columns
- Observations without limit: 10M rows × 13 variables = **130M observations**
- Observations with 1M limit: **1M observations** (first ~77K rows ingested)
- Result: Stops at row 76,923, skips remaining 9,923,077 rows

**When to adjust:**
- **Increase limit** if you have sufficient memory and want more data per file
- **Decrease limit** if experiencing OOM errors on large files
- **Set to null** (unlimited) for small demographic datasets

### Tier 2: Per-Concept Observation Limit

**Purpose:** Enforces hard cap per concept to prevent Java array overflow (Integer.MAX_VALUE = 2.1B)

**Default:** 100,000,000 observations per concept

**How it works:**
1. Each concept (e.g., `\FitBit\HeartRate\`) tracks total observations across ALL files
2. Once a concept reaches the limit, it **rejects** all further observations
3. Rejected observations are tracked in `concept_rejections.jsonl`
4. Prevents finalization failures due to array size limits

**Configuration:**

```properties
# application-ingest.properties
ingest.max-observations-per-concept=100000000
```

**Example:**

Concept: `\phs003463\RECOVER_Adult\DigitalHealthData\HealthKit\Date\`
- Files processed: 1,198 parquet files
- Observations from first 80 files: 100M (limit reached)
- Observations from remaining 1,118 files: **rejected**
- Tracked in: `output/concept_rejections.jsonl`

### Data Loss Tracking

#### concept_rejections.jsonl

Written during finalization to `<output-dir>/concept_rejections.jsonl`:

```jsonl
{"conceptPath":"\\phs003463\\RECOVER_Adult\\DigitalHealthData\\HealthKit\\Date\\","observationsAccepted":100000000,"observationsRejected":25000000,"rejectionRate":20.0,"reason":"exceeded_max_observations_per_concept"}
{"conceptPath":"\\phs003463\\RECOVER_Adult\\DigitalHealthData\\FitBit\\Value\\","observationsAccepted":100000000,"observationsRejected":15000000,"rejectionRate":13.0,"reason":"exceeded_max_observations_per_concept"}
```

Fields:
- `conceptPath`: Full concept path
- `observationsAccepted`: Observations successfully ingested
- `observationsRejected`: Observations rejected due to limit
- `rejectionRate`: Percentage of total observations rejected
- `reason`: Always "exceeded_max_observations_per_concept"

#### Per-File Limit Logging

When a file hits the per-file limit, logs show:

```
INFO  Per-file limit reached: participant_12345.parquet | Limit: 1000000 obs | Generated: 1000000 obs | Rows processed: 76923
```

### Recommendations

**For typical RECOVER datasets (3,000-4,000 participants):**
- Keep default per-file limit: **1M observations**
- Keep default per-concept limit: **100M observations**
- Monitor `concept_rejections.jsonl` after first ingestion
- If many concepts hit 100M, consider increasing per-concept limit or decreasing per-file limit

**For extremely large files (>100K rows per individual):**
- Reduce per-file limit to **500K observations**
- This distributes observations more evenly across files

**For small datasets (<100 files, <10K rows each):**
- Set `maxObservationsPerFile: null` in dataset config (unlimited)
- These datasets won't hit any limits

### Memory Impact

**Per-file limit (1M observations):**
- Arrow buffer size: ~100-200MB per concurrent file
- With 72 concurrent threads: 72 files × 200MB = ~14GB direct memory
- Safe for i7i.24xlarge (768GB total RAM, 143GB MaxDirectMemorySize)

**Per-concept limit (100M observations):**
- In-memory cache: up to 16 concepts × ~190MB = ~3GB heap
- Spool files: compressed to ~50MB each
- Finalization: merges partials, stays under 2.1B array limit

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

### concept_rejections.jsonl (NEW)

Data loss tracking for observations rejected when concepts hit their limit (100M default).

Example:
```jsonl
{"conceptPath":"\\phs003463\\RECOVER_Adult\\DigitalHealthData\\HealthKit\\Date\\","observationsAccepted":100000000,"observationsRejected":25000000,"rejectionRate":20.0,"reason":"exceeded_max_observations_per_concept"}
```

**Use this to:**
- Identify which concepts are hitting limits
- Calculate data loss percentage per concept
- Decide if per-concept limits need adjustment

### ingest.log

Application log with progress, warnings, and errors.

**Per-file limit events appear as:**
```
INFO  Per-file limit reached: participant_12345.parquet | Limit: 1000000 obs | Generated: 1000000 obs | Rows processed: 76923
```

## JVM Configuration & Instance Sizing

The ingest service is designed to run on AWS i7i instances with auto-configured memory and threading based on instance type.

### Auto-Configuration (Terraform Deployment)

When deployed via Terraform (see `hpds-ingest/terraform/`), JVM settings are automatically detected by instance type:

| Instance Type | Total RAM | Heap (Xmx) | Direct Memory | File Threads | Est. Cost/Hour |
|---------------|-----------|------------|---------------|--------------|----------------|
| i7i.24xlarge  | 768 GB    | 614 GB     | 143 GB        | 72           | $7.488         |
| i7i.16xlarge  | 512 GB    | 410 GB     | 96 GB         | 48           | $4.992         |
| i7i.12xlarge  | 384 GB    | 307 GB     | 72 GB         | 36           | $3.744         |
| i7i.8xlarge   | 256 GB    | 205 GB     | 48 GB         | 24           | $2.496         |
| i7i.4xlarge   | 128 GB    | 102 GB     | 24 GB         | 12           | $1.248         |

**Key JVM flags (set automatically by `entrypoint.sh`):**

```bash
# Heap configuration (80% of total RAM)
-Xms<heap>g -Xmx<heap>g

# Direct memory for Arrow off-heap buffers
-XX:MaxDirectMemorySize=<direct>g

# G1 garbage collector with low latency
-XX:+UseG1GC
-XX:MaxGCPauseMillis=200

# OOM protection
-XX:+HeapDumpOnOutOfMemoryError
-XX:HeapDumpPath=/opt/data/output/hpds-oom.hprof
-XX:+ExitOnOutOfMemoryError

# Arrow module access
--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=org.apache.arrow.memory.core,ALL-UNNAMED
--add-opens=java.base/java.lang=ALL-UNNAMED
```

### Performance Expectations by Instance Type

#### i7i.24xlarge (96 vCPU, 768 GB RAM) - **RECOMMENDED for Production**

**Throughput:**
- `dataset_healthkitv2samples` (1.36B obs, 1,198 files): **~3-5 minutes**
- `dataset_fitbitintradaycombined` (3,443 files): **~30-45 minutes** (with 1M per-file limit)
- **Full 18 datasets (RECOVER Adult)**: **~2-3 hours**

**Resource utilization:**
- Memory: 614 GB heap + 143 GB direct = **757 GB total (98% utilization)**
- CPU: **7000-9000%** (70-90 cores actively processing with 72 threads)
- Spool files: **MINIMAL** (concepts rarely exceed 100M with per-file limiting)
- Finalization: **~10-15 minutes** (parallel concept merging)

**Cost:** ~$7.49/hour × 3 hours = **~$22.50 per full ingestion**

#### i7i.12xlarge (48 vCPU, 384 GB RAM)

**Throughput:**
- `dataset_healthkitv2samples`: **~6-10 minutes**
- Full 18 datasets: **~4-6 hours**

**Resource utilization:**
- Memory: 307 GB heap + 72 GB direct
- CPU: **3500-4500%** (35-45 cores with 36 threads)

**Cost:** ~$3.74/hour × 5 hours = **~$18.70 per full ingestion**

#### i7i.8xlarge (32 vCPU, 256 GB RAM)

**Throughput:**
- `dataset_healthkitv2samples`: **~10-15 minutes**
- Full 18 datasets: **~6-8 hours**

**Resource utilization:**
- Memory: 205 GB heap + 48 GB direct
- CPU: **2200-2800%** (22-28 cores with 24 threads)

**Cost:** ~$2.50/hour × 7 hours = **~$17.50 per full ingestion**

#### i7i.4xlarge (16 vCPU, 128 GB RAM)

**Throughput:**
- `dataset_healthkitv2samples`: **~20-30 minutes**
- Full 18 datasets: **~10-14 hours**

**Resource utilization:**
- Memory: 102 GB heap + 24 GB direct
- CPU: **1000-1400%** (10-14 cores with 12 threads)

**Notes:**
- May require reduced per-concept limit (50M instead of 100M) for safety
- Suitable for development/testing, not recommended for production

**Cost:** ~$1.25/hour × 12 hours = **~$15.00 per full ingestion**

### Instance Selection Guidelines

**Choose i7i.24xlarge if:**
- Production ingestion with <4 hour SLA
- Cost per hour matters less than total time
- Processing 3,000+ patient datasets
- Need maximum throughput

**Choose i7i.12xlarge if:**
- Development/staging environment
- Willing to trade 2x time for ~40% cost savings
- Processing smaller datasets (<2,000 patients)

**Choose i7i.8xlarge or smaller if:**
- One-time data migration
- Running during off-hours (time not critical)
- Budget-constrained environments

### Manual Execution (Local Testing)

For local development or testing without Terraform:

```bash
java -Xms64g -Xmx112g \
  -XX:MaxDirectMemorySize=10g \
  -XX:+UseG1GC \
  -XX:MaxGCPauseMillis=200 \
  -XX:+HeapDumpOnOutOfMemoryError \
  -XX:HeapDumpPath=./output/hpds-oom.hprof \
  -XX:+ExitOnOutOfMemoryError \
  --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=org.apache.arrow.memory.core,ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  -jar ingest-service-3.0.0-SNAPSHOT.jar \
  --spring.config.location=file:./configs/application-ingest.properties
```

**Note:** Adjust heap (-Xmx) and direct memory based on available system RAM.

## Performance Characteristics

### Memory usage

- **Heap memory**: Bounded by cache size (default: 50,000 concepts on large instances)
  - Per-concept: ~190MB for 100M observations
  - Cache eviction triggers spool to disk (gzipped)
- **Direct memory**: Used by Apache Arrow for zero-copy parquet reading
  - Per file: ~100-200MB depending on schema width
  - Bounded by MaxDirectMemorySize JVM flag
- **Per-file limit**: Prevents individual files from consuming excessive memory
  - Default: 1M observations per file
  - Configurable per dataset via `maxObservationsPerFile`
- **Per-concept limit**: Prevents Java array overflow during finalization
  - Default: 100M observations per concept
  - Hard rejection (not spooling) prevents unbounded growth

### Disk I/O

- **Sequential writes** during spool phase (compressed with gzip)
- **Sequential reads** during finalize phase
- **Single write** per concept to allObservationsStore (atomic)
- **NVMe storage**: i7i instances include local NVMe SSD for high-performance spooling

### Parallelism

- **File processing**: Bounded virtual threads (12-72 based on instance)
  - Each thread processes one parquet file at a time
  - Virtual threads minimize overhead for I/O-bound operations
- **Concept writes**: Thread-safe via concept-level locking
  - Different concepts can be written concurrently
  - Same concept writes are serialized automatically
- **Finalization**: Parallel concept merging during saveStore()
  - Configurable concurrency (default: 12 concurrent concepts)

## Troubleshooting

### OutOfMemoryError during ingestion

**Symptom:** Process killed with exit code 137 (SIGKILL from OOM killer)

**Diagnosis:**
1. Check CloudWatch logs for last message before kill
2. Look for: "Processing X files with Y bounded virtual threads"
3. If killed during file processing → reduce per-file limit or file concurrency

**Solutions:**

**Option 1: Reduce per-file observation limit (recommended)**
```properties
# application-ingest.properties
ingest.max-observations-per-file=500000  # Reduce from 1M to 500K
```

**Option 2: Reduce file processing threads**
```properties
# application-ingest.properties
ingest.file-processing-threads=36  # Reduce from 72 to 36 (i7i.24xlarge)
```

**Option 3: Increase MaxDirectMemorySize**
```bash
# entrypoint.sh - increase direct memory allocation
DIRECT_MEMORY_GB=$((TOTAL_RAM_GB - HEAP_SIZE_GB - 5))  # Reduce OS overhead from 10GB to 5GB
```

**Option 4: Increase JVM heap**
```bash
java -Xmx16g -jar ingest-service.jar ...
```

### Concept hits observation limit during ingestion

**Symptom:** Logs show "Rejecting observation for concept '...' (limit reached: 100000000 total observations)"

**Diagnosis:**
1. Check `concept_rejections.jsonl` after ingestion completes
2. Identify concepts with high rejection rates
3. Common culprits: timestamp columns, high-frequency sensor data

**Solutions:**

**Option 1: Increase per-concept limit**
```properties
# application-ingest.properties
ingest.max-observations-per-concept=200000000  # Increase from 100M to 200M
```

**Option 2: Reduce per-file limit (spreads observations more evenly)**
```properties
# application-ingest.properties
ingest.max-observations-per-file=500000  # Reduce from 1M to 500K
```

**Option 3: Filter high-cardinality columns**
Remove timestamp or high-frequency columns from dataset config if not needed for queries.

### Finalization fails with "Java array limit" error

**Symptom:** Error message: "Concept exceeds Java array limit: ... observations > 2.1B limit"

**Root cause:** Despite per-concept limit, estimation is conservative and may fail on edge cases

**Solution:**
```properties
# Reduce per-concept limit to stay well under 2.1B
ingest.max-observations-per-concept=50000000  # Reduce to 50M for safety
```

### File processing unexpectedly stops early

**Symptom:** Logs show "Per-file limit reached" for many files

**Diagnosis:** Per-file limit is too low for your data density

**Solution:**
```properties
# Increase per-file limit
ingest.max-observations-per-file=2000000  # Increase from 1M to 2M
```

Or disable per-file limit for specific small datasets:
```jsonl
{"dataset":"enrolledparticipants","maxObservationsPerFile":null,...}
```

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

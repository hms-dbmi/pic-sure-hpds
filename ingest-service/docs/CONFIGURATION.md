# Configuration Guide

Complete reference for configuring HPDS Ingest Service.

## Application Properties

Configuration file: `configs/application-ingest.properties`

### Observation Limits

```properties
# Per-file observation limit (NEW in v3.1.0)
# Prevents OOM when processing large individual files
ingest.max-observations-per-file=1000000

# Per-concept observation limit
# Prevents Java array overflow (Integer.MAX_VALUE = 2.1B)
ingest.max-observations-per-concept=100000000
```

**Recommendations:**
- **Per-file**: 1M for large datasets, `null` for small datasets
- **Per-concept**: 100M default, increase to 200M if concepts hit limit

### Performance Tuning

```properties
# Parallel file processing threads
# Auto-overridden by Terraform based on instance type
ingest.file-processing-threads=216

# In-memory concept cache size
ingest.store-cache-size=50000

# Rows per batch during parquet reading
ingest.batch-size=100000
```

### Data Sources

```properties
# Parquet data location
ingest.parquet-base-dir=/opt/data/input/DHDR

# Dataset configuration (JSONL format)
ingest.parquet-config-path=/opt/data/configs/full_test_study_ingest.jsonl

# Legacy CSV data location
ingest.csv-dir=/opt/data/input/
```

### Output Configuration

```properties
# Final observation store output
ingest.output-dir=/opt/data/output/

# Temporary spool directory (cleared after finalization)
ingest.spool-dir=/opt/data/spool

# Failure tracking file
ingest.failure-file=${ingest.output-dir}/failures.jsonl
```

### Logging

```properties
logging.file.name=${ingest.output-dir}/ingest.log
logging.level.root=INFO
logging.level.edu.harvard.hms.dbmi.avillach.hpds=INFO
logging.level.org.apache.parquet=WARN
logging.level.org.apache.hadoop=WARN
```

## Dataset Configuration (JSONL)

Each line in the JSONL file represents one dataset configuration.

### Example

```jsonl
{"dataset":"dataset_fitbitintradaycombined","device":"FitBit","participantIdColumn":"ParticipantIdentifier","timestampColumn":"DateTime","conceptPathPrefix":["phs003463","RECOVER_Adult","DigitalHealthData","FitBit","Intradaycombined"],"variables":[{"column":"Type","label":"Type"},{"column":"DateTime","label":"DateTime"},{"column":"Value","label":"Value"}]}
```

### Required Fields

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `dataset` | String | Dataset name (subdirectory under parquet base dir) | `"dataset_fitbitintradaycombined"` |
| `device` | String | Device name for concept path | `"FitBit"` |
| `participantIdColumn` | String | Column containing participant ID | `"ParticipantIdentifier"` |
| `timestampColumn` | String | Column containing timestamp, or `"none"` | `"DateTime"` |
| `conceptPathPrefix` | Array | Concept path hierarchy | `["phs003463","RECOVER_Adult"]` |
| `variables` | Array | Variable configurations (see below) | See below |

### Optional Fields

| Field | Type | Description | Default |
|-------|------|-------------|---------|
| `maxObservationsPerFile` | Long | Per-file observation limit override | Global config value (1M) |

**Example with override:**
```jsonl
{"dataset":"enrolledparticipants","maxObservationsPerFile":null,"participantIdColumn":"ParticipantIdentifier",...}
```

### Variable Configuration

Each variable object in the `variables` array:

| Field | Type | Description | Values |
|-------|------|-------------|--------|
| `column` | String | Parquet column name | Required |
| `label` | String | Human-readable label for concept path | Required |
| `forceType` | String | Force type interpretation | `"NUMERIC"`, `"TEXT"`, or omit for auto-detect |

**Example:**
```json
{
  "column": "HeartRate",
  "label": "Heart Rate (bpm)",
  "forceType": "NUMERIC"
}
```

## Concept Path Format

HPDS uses backslash-delimited concept paths with trailing delimiter:

```
\<prefix_component_1>\<prefix_component_2>\<device>\<variable_label>\
```

**Example:**
```
\phs003463\RECOVER_Adult\DigitalHealthData\FitBit\HeartRate\
```

Generated from:
- `conceptPathPrefix`: `["phs003463", "RECOVER_Adult", "DigitalHealthData"]`
- `device`: `"FitBit"`
- `variable.label`: `"HeartRate"`

## Observation Limits Deep Dive

### Two-Tier Strategy

**Tier 1: Per-File Limit**
- Applied during file processing
- Stops reading file when limit reached
- Deterministic (same rows always ingested)
- Default: 1M observations per file

**Tier 2: Per-Concept Limit**
- Applied across all files
- Rejects observations when concept total ≥ limit
- Tracked in `concept_rejections.jsonl`
- Default: 100M observations per concept

### Calculating Observations

Observations = Rows × Variables

**Example 1: FitBit Intradaycombined**
- Rows per file: 100,000
- Variables: 13
- Observations per file: 1.3M
- **Result**: Hits 1M per-file limit at row 76,923

**Example 2: Demographics**
- Rows per file: 1
- Variables: 10
- Observations per file: 10
- **Result**: Never hits per-file limit

### When to Override Limits

**Increase per-file limit** if:
- High data loss (many files hitting limit)
- Sufficient memory available
- Want more comprehensive data coverage

**Decrease per-file limit** if:
- OOM errors during ingestion
- Want to spread observations more evenly

**Set per-file limit to `null`** if:
- Small dataset (<100 files)
- Low row count (<10K rows per file)
- Few variables (<5)

## Environment Variables

When deployed via Terraform, these environment variables override properties:

| Variable | Purpose | Example |
|----------|---------|---------|
| `HEAP_SIZE_GB` | JVM heap memory (Xmx) | `614` |
| `FILE_PROCESSING_THREADS` | Parallel file threads | `72` |
| `MAX_OBS_PER_CONCEPT` | Per-concept limit | `100000000` |
| `SPOOL_DIR` | Temporary spool directory | `/mnt/nvme/spool` |

## Configuration Examples

### High-Frequency Sensor Data

```jsonl
{"dataset":"dataset_healthkitv2samples","device":"HealthKit","participantIdColumn":"ParticipantIdentifier","timestampColumn":"StartDate","maxObservationsPerFile":500000,"conceptPathPrefix":["phs003463","RECOVER_Adult","DigitalHealthData","HealthKit","V2samples"],"variables":[{"column":"Value","label":"Value","forceType":"NUMERIC"}]}
```

**Strategy**: Reduced per-file limit (500K) to prevent OOM on high-frequency data

### Relational Child Table (No Timestamp)

```jsonl
{"dataset":"enrolledparticipants","device":"Unknown","participantIdColumn":"ParticipantIdentifier","timestampColumn":"none","maxObservationsPerFile":null,"conceptPathPrefix":["phs003463","RECOVER_Adult","Demographics"],"variables":[{"column":"EnrollmentDate","label":"EnrollmentDate"}]}
```

**Strategy**: Unlimited per-file (small dataset), no timestamp column

### Multi-Variable Dataset

```jsonl
{"dataset":"dataset_fitbitintradaycombined","device":"FitBit","participantIdColumn":"ParticipantIdentifier","timestampColumn":"DateTime","conceptPathPrefix":["phs003463","RECOVER_Adult","DigitalHealthData","FitBit","Intradaycombined"],"variables":[{"column":"Type","label":"Type"},{"column":"DateTime","label":"DateTime"},{"column":"Value","label":"Value","forceType":"NUMERIC"},{"column":"Calories","label":"Calories","forceType":"NUMERIC"},{"column":"Distance","label":"Distance","forceType":"NUMERIC"}]}
```

**Strategy**: Multiple variables with explicit numeric types for measurements

## Validation

### Check Configuration Syntax

```bash
# Validate JSONL syntax
jq empty configs/dataset_config.jsonl

# Pretty-print first dataset
head -1 configs/dataset_config.jsonl | jq .
```

### Test Configuration

```bash
# Run with single dataset for testing
echo '{"dataset":"test_dataset",...}' > test_config.jsonl

java -jar ingest-service.jar \
  --ingest.parquet-config-path=test_config.jsonl \
  --ingest.output-dir=./test_output
```

### Common Configuration Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `SCHEMA_MISMATCH` | Column in config not in parquet file | Verify column names with `parquet-tools schema` |
| `MISSING_TIMESTAMP_COLUMN` | Timestamp column not found | Check `timestampColumn` value or set to `"none"` |
| `Invalid JSON` | Malformed JSONL | Validate with `jq` |
| `Dataset directory not found` | Wrong `dataset` value | Verify subdirectory exists under `parquet-base-dir` |

## Next Steps

- **Architecture**: See [Architecture Guide](ARCHITECTURE.md) for system design
- **Deployment**: See [Deployment Guide](DEPLOYMENT.md) for JVM tuning and instance sizing

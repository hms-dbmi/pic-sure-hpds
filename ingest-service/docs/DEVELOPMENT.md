# Development Guide

Maven builds, testing, local development, and debugging for HPDS Ingest Service.

## Prerequisites

```bash
# Required
Java 17+
Maven 3.8+

# Optional (for parquet inspection)
parquet-tools
```

## Building

### Build All Modules

From `pic-sure-hpds/` root:

```bash
mvn clean install
```

### Build Ingest Service Only

```bash
# Build with dependencies
mvn -pl :ingest-service -am clean package

# Build without tests (faster)
mvn -pl :ingest-service -am clean package -DskipTests
```

### Build Writer Adapter Only

```bash
mvn -pl :hpds-writer-adapter -am clean install
```

## Testing

### Run All Tests

```bash
mvn -pl :hpds-writer-adapter,:ingest-service test
```

### Run Specific Test Class

```bash
mvn -pl :ingest-service test -Dtest=ParquetObservationProducerTest
```

### Run Single Test Method

```bash
mvn -pl :ingest-service test \
  -Dtest=ParquetObservationProducerTest#testPerFileObservationLimit
```

### Test with Debug Logging

```bash
mvn test -Dlogging.level.root=DEBUG
```

## Local Development

### Setup Test Data

```bash
# Create test directory structure
mkdir -p /tmp/hpds-test/{input,output,spool,configs}

# Copy sample configs
cp configs/application-ingest.properties /tmp/hpds-test/configs/
cp configs/dataset_config.jsonl /tmp/hpds-test/configs/

# Link or copy data
ln -s /path/to/parquet/data /tmp/hpds-test/input/DHDR
```

### Run Locally

```bash
java -Xms8g -Xmx16g \
  -XX:MaxDirectMemorySize=4g \
  -XX:+HeapDumpOnOutOfMemoryError \
  -XX:HeapDumpPath=/tmp/hpds-oom.hprof \
  --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=org.apache.arrow.memory.core,ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  -jar ingest-service/target/ingest-service-3.0.0-SNAPSHOT.jar \
  --spring.config.location=file:/tmp/hpds-test/configs/application-ingest.properties \
  --ingest.parquet-base-dir=/tmp/hpds-test/input/DHDR \
  --ingest.output-dir=/tmp/hpds-test/output \
  --ingest.spool-dir=/tmp/hpds-test/spool
```

### Run with Debug Port

```bash
java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005 \
  -Xms8g -Xmx16g \
  -jar ingest-service/target/ingest-service-3.0.0-SNAPSHOT.jar \
  --spring.config.location=file:/tmp/hpds-test/configs/application-ingest.properties
```

Connect IntelliJ/Eclipse debugger to `localhost:5005`

### Override Configuration

```bash
# Override specific properties via command line
java -jar ingest-service.jar \
  --spring.config.location=file:./configs/application-ingest.properties \
  --ingest.max-observations-per-file=500000 \
  --ingest.file-processing-threads=4 \
  --logging.level.edu.harvard.hms.dbmi.avillach.hpds=DEBUG
```

## Testing Scenarios

### Test Per-File Observation Limit

**Objective**: Verify file processing stops at configured limit

```bash
# Configure low limit
echo "ingest.max-observations-per-file=1000" >> test.properties

# Run on dataset with large files
java -jar ingest-service.jar \
  --spring.config.location=file:test.properties

# Verify limit hit in logs
grep "Per-file limit reached" output/ingest.log
```

### Test Per-Concept Observation Limit

**Objective**: Verify concepts reject observations when limit reached

```bash
# Configure low limit
echo "ingest.max-observations-per-concept=10000" >> test.properties

# Run ingestion
java -jar ingest-service.jar \
  --spring.config.location=file:test.properties

# Check rejection tracking
jq '.' output/concept_rejections.jsonl
```

### Test Multi-Dataset Ingestion

**Objective**: Verify multiple datasets process concurrently

```jsonl
{"dataset":"dataset1",...}
{"dataset":"dataset2",...}
{"dataset":"dataset3",...}
```

```bash
# Monitor log for concurrent processing
tail -f output/ingest.log | grep "Processing.*files"
```

### Test Failure Tracking

**Objective**: Verify all failures tracked in failures.jsonl

```bash
# Introduce malformed data
# Run ingestion
# Check failures
jq -r '.reasonCode' output/failures.jsonl | sort | uniq -c
```

### Test Spool and Finalize

**Objective**: Verify cache eviction and finalization

```bash
# Configure small cache
echo "ingest.store-cache-size=10" >> test.properties

# Run ingestion
# Monitor spool directory
watch -n 1 'ls -lh /tmp/hpds-test/spool | wc -l'

# Verify cleanup after finalization
ls /tmp/hpds-test/spool
```

## Debugging

### Enable Debug Logging

```properties
# application-ingest.properties
logging.level.root=INFO
logging.level.edu.harvard.hms.dbmi.avillach.hpds=DEBUG
logging.level.edu.harvard.hms.dbmi.avillach.hpds.writer=TRACE
logging.level.org.apache.parquet=DEBUG
```

### Inspect Heap Dump

```bash
# After OOM, analyze heap dump
jhat /tmp/hpds-oom.hprof

# Or use Eclipse MAT
mat /tmp/hpds-oom.hprof
```

### Profile Performance

```bash
# Enable JFR (Java Flight Recorder)
java -XX:StartFlightRecording=filename=recording.jfr,duration=60s \
  -jar ingest-service.jar

# Analyze recording
jfr print --events jdk.ObjectAllocationSample recording.jfr
```

### Monitor with JConsole

```bash
# Start with JMX enabled
java -Dcom.sun.management.jmxremote \
  -Dcom.sun.management.jmxremote.port=9010 \
  -Dcom.sun.management.jmxremote.authenticate=false \
  -Dcom.sun.management.jmxremote.ssl=false \
  -jar ingest-service.jar

# Connect JConsole
jconsole localhost:9010
```

### Inspect Parquet Files

```bash
# View schema
parquet-tools schema /path/to/file.parquet

# View metadata
parquet-tools meta /path/to/file.parquet

# Dump first 10 rows
parquet-tools head -n 10 /path/to/file.parquet

# Count rows
parquet-tools rowcount /path/to/file.parquet

# View specific columns
parquet-tools dump --column ParticipantIdentifier /path/to/file.parquet
```

### Inspect Output Files

```bash
# View columnMeta.csv
cat output/columnMeta.csv | column -t -s,

# Count concepts
wc -l output/columnMeta.csv

# Check specific concept
grep "FitBit\\\\HeartRate" output/columnMeta.csv

# Analyze failures
jq -r '[.reasonCode, .dataset] | @tsv' output/failures.jsonl | sort | uniq -c

# Check data loss
jq -r '[.conceptPath, .rejectionRate] | @tsv' output/concept_rejections.jsonl | sort -t$'\t' -k2 -nr
```

## Testing Best Practices

### Unit Testing

**ParquetObservationProducer tests:**
```java
@Test
void testPerFileObservationLimit() {
    producer.setPerFileObservationLimit(1000);

    List<ObservationRow> observations = producer.processFile(testFile);

    assertThat(observations).hasSizeLessThanOrEqualTo(1000);
}
```

**SpoolingLoadingStore tests:**
```java
@Test
void testPerConceptLimitRejectsObservations() {
    store.setMaxObservationsPerConcept(100);

    // Add 150 observations
    for (int i = 0; i < 150; i++) {
        store.addObservation(createTestObservation());
    }

    store.saveStore();

    // Verify only 100 accepted
    assertThat(getRejectionCount()).isEqualTo(50);
}
```

### Integration Testing

**Full ingestion test:**
```java
@SpringBootTest
class IngestServiceIntegrationTest {
    @Test
    void testFullIngestionPipeline() {
        // Setup test data
        // Run ingestion
        // Verify output files
        // Check failures
        // Validate columnMeta
    }
}
```

### Performance Testing

**Measure throughput:**
```bash
#!/bin/bash
START=$(date +%s)
java -jar ingest-service.jar --spring.config.location=file:test.properties
END=$(date +%s)
DURATION=$((END - START))

OBSERVATIONS=$(jq -s 'map(.observationCount) | add' output/columnMeta.csv)
THROUGHPUT=$((OBSERVATIONS / DURATION))

echo "Duration: ${DURATION}s"
echo "Observations: ${OBSERVATIONS}"
echo "Throughput: ${THROUGHPUT} obs/s"
```

## Code Style

### Formatting

```bash
# Format code (if configured)
mvn spotless:apply

# Check formatting
mvn spotless:check
```

### Static Analysis

```bash
# Run SpotBugs
mvn spotbugs:check

# Run Checkstyle
mvn checkstyle:check
```

## Contributing

### Adding New Features

1. **Add tests first** (TDD)
   ```bash
   # Create test
   vim ingest-service/src/test/java/...Test.java

   # Run test (should fail)
   mvn test -Dtest=NewFeatureTest
   ```

2. **Implement feature**
   ```bash
   vim ingest-service/src/main/java/...
   ```

3. **Verify tests pass**
   ```bash
   mvn test
   ```

4. **Update documentation**
   ```bash
   vim docs/CONFIGURATION.md
   ```

### Testing Changes

```bash
# Run full test suite
mvn clean verify

# Build and test with integration tests
mvn clean install -Pintegration-tests

# Local smoke test
./scripts/local-test.sh
```

## Common Issues

### Maven Build Failures

**Issue**: `Could not resolve dependencies`

**Solution**: Clear local repository
```bash
rm -rf ~/.m2/repository/edu/harvard/hms/dbmi/avillach/hpds
mvn clean install
```

### Arrow Memory Errors

**Issue**: `OutOfDirectMemoryError`

**Solution**: Increase MaxDirectMemorySize
```bash
java -XX:MaxDirectMemorySize=8g -jar ingest-service.jar
```

### Spring Boot Conflicts

**Issue**: `BeanCreationException`

**Solution**: Check configuration precedence
```bash
# Explicitly set config location
--spring.config.location=file:./configs/application-ingest.properties
```

## Next Steps

- **Architecture**: See [Architecture Guide](ARCHITECTURE.md) for system internals
- **Configuration**: See [Configuration Guide](CONFIGURATION.md) for all settings
- **Operations**: See [Operations Guide](OPERATIONS.md) for production monitoring

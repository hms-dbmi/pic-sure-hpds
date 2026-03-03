# HPDS Ingest Service

Spring Boot application that processes Parquet and CSV data into HPDS observation stores. Part of the PIC-SURE HPDS data infrastructure.

## Overview

This is the core ingestion service component. It provides the data processing logic, storage engines, and observation handling for HPDS data ingestion workflows.

**Key Features:**
- Multi-source ingestion (Parquet + CSV) without global concept ordering
- SpoolingLoadingStore with memory-bounded observation processing
- Configurable limits at file and concept levels
- Apache Arrow zero-copy Parquet reading
- Virtual thread-based parallel processing
- JSONL audit reporting for data loss tracking

**Technology Stack:**
- Java 25 (Amazon Corretto)
- Spring Boot 3.x
- Apache Arrow (Parquet processing)
- Project Loom (virtual threads)

## Documentation

| Document | Description |
|----------|-------------|
| **[Configuration Guide](docs/CONFIGURATION.md)** | Application properties, dataset configs, observation limits |
| **[Architecture Guide](docs/ARCHITECTURE.md)** | System design, module structure, SpoolingLoadingStore |
| **[Development Guide](docs/DEVELOPMENT.md)** | Maven builds, testing, local development |
| **[Spooling Workflow](docs/SPOOLING_WORKFLOW.md)** | Complete workflow diagram from ingestion to finalization |
| **[Spool Estimation Bug Fix](docs/SPOOL_ESTIMATION_BUG.md)** | Details on the fixed spool file estimation bug |

**For deployment and operations documentation**, see the `hpds-ingest/` orchestration layer.

## Project Structure

```
ingest-service/
├── src/main/java/
│   └── edu/harvard/hms/dbmi/avillach/hpds/ingest/
│       ├── IngestServiceApplication.java      # Spring Boot entry point
│       ├── config/                             # Configuration classes
│       ├── storage/                            # SpoolingLoadingStore, ColumnStore
│       ├── processing/                         # File processors, loaders
│       └── models/                             # Data models, DTOs
├── src/main/resources/
│   └── application.properties                  # Default application config
├── src/test/                                   # Unit and integration tests
└── pom.xml                                     # Maven configuration
```

## Quick Start

### Build

```bash
# Build this module only
mvn -pl :ingest-service -am clean package

# Build all modules
mvn clean package
```

### Run Locally

```bash
java -jar ingest-service/target/ingest-service-3.0.0-SNAPSHOT.jar \
  --spring.config.location=file:./configs/application-ingest.properties
```

### Configuration

See [CONFIGURATION.md](docs/CONFIGURATION.md) for complete configuration reference.

Key properties:
- `hpds.ingest.datasets` - Dataset configurations (CSV/Parquet sources)
- `hpds.ingest.observation-limit.per-file` - Per-file observation limit (default: 1M)
- `hpds.ingest.observation-limit.per-concept` - Per-concept limit (default: 100M)
- `hpds.ingest.processing.file-threads` - Parallel file processing threads

## Development

### Testing

```bash
# Run all tests
mvn test

# Run specific test class
mvn test -Dtest=SpoolingLoadingStoreTest

# Skip tests during build
mvn package -DskipTests
```

### Debugging

```bash
java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 \
  -jar ingest-service/target/ingest-service-3.0.0-SNAPSHOT.jar
```

### Code Style

```bash
# Format code
mvn spotless:apply

# Check formatting
mvn spotless:check
```

## Architecture

See [ARCHITECTURE.md](docs/ARCHITECTURE.md) for detailed system design.

**Key Components:**
- **SpoolingLoadingStore** - Memory-bounded storage with cache eviction
- **ParquetFileProcessor** - Arrow-based zero-copy Parquet reading
- **CSVFileProcessor** - Efficient CSV parsing with observation limits
- **ColumnStore** - Persistent observation storage

**Data Flow:**
1. Dataset configurations loaded from properties
2. File processors read Parquet/CSV in parallel
3. SpoolingLoadingStore accumulates observations
4. Cache eviction triggers finalization
5. ColumnStore writes to disk
6. JSONL reports track data loss

## Important Notes

### Spool File Estimation (Fixed)

Earlier versions had a bug where the system would incorrectly estimate total observations by assuming every spool file was full. This caused false "exceeds Java array limit" errors for concepts with < 100K observations.

**Symptoms of the old bug:**
- Error claiming billions of observations for concepts with < 100K
- "42 partials × 100M ≈ 4.20B observations" type messages
- Finalization failures on valid data

**This has been fixed.** The system now uses the actual observation count tracked during ingestion (`meta.totalObservationCount`), which is 100% accurate.

See [SPOOL_ESTIMATION_BUG.md](docs/SPOOL_ESTIMATION_BUG.md) for technical details.

## Deployment

This service is designed to run in containerized environments with large memory and NVMe storage.

**For production deployment:**
- See `hpds-ingest/` orchestration layer
- Terraform infrastructure automation
- Docker containerization
- CloudWatch Logs integration

**Deployment documentation:**
- `hpds-ingest/docs/QUICKSTART.md` - Deployment workflow
- `hpds-ingest/docs/DEPLOYMENT.md` - Terraform and AWS setup
- `hpds-ingest/docs/TERRAFORM.md` - Infrastructure reference
- `hpds-ingest/docs/OPERATIONS.md` - Monitoring and troubleshooting

## Related Projects

**Multi-repo structure:**
- `pic-sure-hpds/ingest-service/` - This service (ingestion logic)
- `hpds-ingest/` - Orchestration layer (Terraform, Docker, Makefile)
- `pic-sure-hpds/service/` - HPDS query service

## License

Copyright © 2026 Harvard Medical School, Department of Biomedical Informatics

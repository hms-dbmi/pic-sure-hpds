# HPDS Ingest Service Documentation

Technical documentation for the HPDS ingest service core components.

## Documentation Index

### Configuration

- **[CONFIGURATION.md](CONFIGURATION.md)** - Application configuration reference
  - Application properties
  - Dataset configurations
  - Observation limits
  - Processing settings
  - S3 and storage configuration

### Architecture

- **[ARCHITECTURE.md](ARCHITECTURE.md)** - System design and internals
  - SpoolingLoadingStore design
  - Data flow and processing
  - Memory management
  - Parallel processing architecture
  - Storage engines

### Development

- **[DEVELOPMENT.md](DEVELOPMENT.md)** - Developer guide
  - Maven build instructions
  - Testing strategies
  - Debugging techniques
  - Code organization
  - Contributing guidelines

## Quick Reference

### Build Commands

```bash
# Build this module only
mvn -pl :ingest-service -am clean package

# Run tests
mvn test

# Run specific test
mvn test -Dtest=SpoolingLoadingStoreTest
```

### Run Locally

```bash
java -jar ingest-service/target/ingest-service-3.0.0-SNAPSHOT.jar \
  --spring.config.location=file:./configs/application-ingest.properties
```

## Key Concepts

### SpoolingLoadingStore

Memory-bounded storage engine that allows cache eviction and finalization without global concept ordering.

**Key features:**
- Per-file observation limits (default: 1M)
- Per-concept observation limits (default: 100M)
- Automatic cache eviction
- Parallel finalization

See [ARCHITECTURE.md](ARCHITECTURE.md) for details.

### Multi-Source Ingestion

Process Parquet and CSV files from different datasets without requiring unified `allConcepts.csv`.

**Supported sources:**
- Parquet files (Apache Arrow zero-copy reading)
- CSV files (streaming processing)
- Mixed dataset types

See [CONFIGURATION.md](CONFIGURATION.md) for dataset configuration.

## Related Documentation

### Deployment and Operations

For deploying and running this service in production:

- `hpds-ingest/README.md` - Orchestration layer overview
- `hpds-ingest/docs/QUICKSTART.md` - Deployment workflow
- `hpds-ingest/docs/DEPLOYMENT.md` - AWS and infrastructure setup
- `hpds-ingest/docs/TERRAFORM.md` - Terraform reference
- `hpds-ingest/docs/OPERATIONS.md` - Monitoring and troubleshooting

### External Resources

- [Spring Boot Documentation](https://docs.spring.io/spring-boot/docs/current/reference/html/)
- [Apache Arrow Java Documentation](https://arrow.apache.org/docs/java/)
- [Project Loom (Virtual Threads)](https://openjdk.org/jeps/444)

## Support

**For configuration questions:**
- See [CONFIGURATION.md](CONFIGURATION.md)
- Review `application-ingest.properties` examples

**For architecture questions:**
- See [ARCHITECTURE.md](ARCHITECTURE.md)
- Review source code documentation

**For development questions:**
- See [DEVELOPMENT.md](DEVELOPMENT.md)
- Check test examples

package edu.harvard.hms.dbmi.avillach.hpds.ingest;

import edu.harvard.hms.dbmi.avillach.hpds.ingest.config.IngestConfig;
import edu.harvard.hms.dbmi.avillach.hpds.ingest.config.ParquetDatasetConfig;
import edu.harvard.hms.dbmi.avillach.hpds.ingest.failure.FailureSink;
import edu.harvard.hms.dbmi.avillach.hpds.ingest.mapping.DelimitedFileMappingLoader;
import edu.harvard.hms.dbmi.avillach.hpds.ingest.mapping.MappingChainPatientIdResolver;
import edu.harvard.hms.dbmi.avillach.hpds.ingest.mapping.MappingFileInspector;
import edu.harvard.hms.dbmi.avillach.hpds.ingest.mapping.PatientIdResolver;
import edu.harvard.hms.dbmi.avillach.hpds.ingest.producer.CsvObservationProducer;
import edu.harvard.hms.dbmi.avillach.hpds.ingest.producer.ParquetObservationProducer;
import edu.harvard.hms.dbmi.avillach.hpds.writer.HPDSWriterAdapter;
import edu.harvard.hms.dbmi.avillach.hpds.writer.ObservationRow;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Multi-source HPDS ingestion service.
 *
 * Orchestrates:
 * - Parquet dataset ingestion (DHDR)
 * - CSV file ingestion (legacy studies)
 * - Failure tracking
 * - HPDS store finalization
 *
 * Run with:
 * java -jar ingest-service.jar \
 *   --ingest.parquet.base-dir=/path/to/DHDR \
 *   --ingest.parquet.config-path=/path/to/datasets.jsonl \
 *   --ingest.mapping.telemetry-report-file=/path/to/telemetry_report.txt \
 *   --ingest.mapping.patient-mapping-file=/path/to/patient_mapping.csv \
 *   --ingest.output.dir=/opt/local/hpds/
 *
 * Legacy CLI args (deprecated but supported):
 *   --parquet.dir, --parquet.config, --mapping.telemetryReportFile, --mapping.patientMappingFile, etc.
 */
@SpringBootApplication(exclude = {
    org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration.class,
    org.springframework.boot.autoconfigure.batch.BatchAutoConfiguration.class
})
@ConfigurationPropertiesScan("edu.harvard.hms.dbmi.avillach.hpds.ingest.config")
public class IngestServiceApplication implements CommandLineRunner {
    private static final Logger log = LoggerFactory.getLogger(IngestServiceApplication.class);

    private final IngestConfig config;

    public IngestServiceApplication(IngestConfig config) {
        this.config = config;
    }

    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(IngestServiceApplication.class);
        app.run(args);
    }

    @Override
    public void run(String... args) throws Exception {
        // Generate run ID and set up MDC for structured logging
        String runId = UUID.randomUUID().toString();
        MDC.put("run_id", runId);
        MDC.put("service_name", "hpds-ingest-service");
        MDC.put("service_version", "3.0.0-SNAPSHOT");

        // Check for environment variable (set by container)
        String env = System.getenv("ENVIRONMENT");
        if (env != null && !env.isBlank()) {
            MDC.put("environment", env);
        }

        try {
            // Emit structured start event
            log.atInfo()
                .addKeyValue("event_type", "ingest.started")
                .addKeyValue("event_schema_version", "1.0")
                .log("Starting HPDS multi-source ingestion");

            // Check for deprecated CLI args and warn
            checkDeprecatedArgs(args);

        // Initialize patient ID resolver
        PatientIdResolver patientIdResolver = initializePatientIdResolver(
            config.getMappingTelemetryReportFile(),
            config.getMappingPatientMappingFile(),
            config.isMappingInspectOnly()
        );

        if (config.isMappingInspectOnly()) {
            log.info("Inspection mode complete. Exiting.");
            return;
        }

        Instant startTime = Instant.now();
        AtomicLong totalObservations = new AtomicLong(0);

        // Get or generate encryption key
        String encryptionKeyName = config.getEncryptionKeyName();
        if (encryptionKeyName == null || encryptionKeyName.isBlank()) {
            encryptionKeyName = "encryption_key";
        }
        String keyFilePath = config.getOutputDir() + "/encryption_key";
        Path keyPath = Path.of(keyFilePath);
        if (!Files.exists(keyPath)) {
            generateAndSaveEncryptionKey(encryptionKeyName, keyFilePath);
        } else {
            log.info("Using existing encryption key: {}", keyFilePath);
            edu.harvard.hms.dbmi.avillach.hpds.crypto.Crypto.loadKey(encryptionKeyName, keyFilePath);
        }

        try (FailureSink failureSink = new FailureSink(Path.of(config.getFailureFile()));
             HPDSWriterAdapter writer = new HPDSWriterAdapter(
                 Path.of(config.getSpoolDir()),
                 config.getOutputDir(),
                 encryptionKeyName,
                 config.getStoreCacheSize(),
                 config.getMaxObservationsPerConcept(),
                 config.getFinalizationConcurrency(),
                 config.getFinalizationChunkSize(),
                 config.isDisableAdaptiveDegradation(),
                 false)) {

            // Process Parquet datasets
            MDC.put("stage", "parquet");
            try {
                log.atInfo()
                    .addKeyValue("event_type", "stage.started")
                    .addKeyValue("event_schema_version", "1.0")
                    .addKeyValue("stage", "parquet")
                    .log("Processing Parquet datasets from config: {}", config.getParquetConfigPath());

                List<ParquetDatasetConfig> datasetConfigs = processParquetDatasets(
                    runId,
                    config.getParquetBaseDir(),
                    config.getParquetConfigPath(),
                    failureSink,
                    writer,
                    totalObservations,
                    patientIdResolver
                );

                // Warn if no datasets were processed
                if (datasetConfigs.isEmpty()) {
                    log.atError()
                        .addKeyValue("event_type", "warning.no.dataset.configs")
                        .addKeyValue("event_schema_version", "1.0")
                        .addKeyValue("config_path", config.getParquetConfigPath())
                        .log("!!! NO DATASET CONFIGS FOUND - ZERO INGESTION !!!");
                }
            } finally {
                MDC.remove("stage");
            }

            // Process CSV files (optional)
            if (config.getCsvDir() != null) {
                MDC.put("stage", "csv");
                try {
                    log.atInfo()
                        .addKeyValue("event_type", "stage.started")
                        .addKeyValue("event_schema_version", "1.0")
                        .addKeyValue("stage", "csv")
                        .log("Processing CSV files from: {}", config.getCsvDir());

                    processCsvFiles(runId, config.getCsvDir(), failureSink, writer, totalObservations);
                } finally {
                    MDC.remove("stage");
                }
            }

            // Finalize stores
            MDC.put("stage", "finalization");
            try {
                log.atInfo()
                    .addKeyValue("event_type", "stage.started")
                    .addKeyValue("event_schema_version", "1.0")
                    .addKeyValue("stage", "finalization")
                    .log("Finalizing HPDS stores...");

                writer.closeAndSave();
            } finally {
                MDC.remove("stage");
            }

            Instant endTime = Instant.now();
            long durationSeconds = endTime.getEpochSecond() - startTime.getEpochSecond();

            // Log completion with warning if zero ingestion
            if (totalObservations.get() == 0 && failureSink.getTotalFailures() == 0) {
                log.atWarn()
                    .addKeyValue("event_type", "warning.zero.ingestion")
                    .addKeyValue("event_schema_version", "1.0")
                    .addKeyValue("total_observations", 0L)
                    .addKeyValue("total_failures", 0L)
                    .addKeyValue("config_path", config.getParquetConfigPath())
                    .addKeyValue("possible_causes", java.util.List.of(
                        "Config path may not contain any .jsonl files",
                        "Dataset names in config may not match directory names",
                        "Parquet base directory may be incorrect"
                    ))
                    .log("=== INGESTION COMPLETE (BUT WITH ZERO OBSERVATIONS) - likely configuration problem ===");
            } else {
                // Emit structured completion event
                log.atInfo()
                    .addKeyValue("event_type", "ingest.completed")
                    .addKeyValue("event_schema_version", "1.0")
                    .addKeyValue("status", "completed")
                    .addKeyValue("total_observations", totalObservations.get())
                    .addKeyValue("total_patients", writer.getPatientCount())
                    .addKeyValue("total_failures", failureSink.getTotalFailures())
                    .addKeyValue("duration_seconds", durationSeconds)
                    .log("=== INGESTION COMPLETE ===");
            }

            // Traditional log lines for console readability (also in JSON)
            log.info("Run ID: {}", runId);
            log.info("Total observations: {}", totalObservations.get());
            log.info("Total patients: {}", writer.getPatientCount());
            log.info("Total failures: {}", failureSink.getTotalFailures());
            log.info("Duration: {} seconds", durationSeconds);
            log.info("Output: {}", config.getOutputDir());
            log.info("Failures: {}", config.getFailureFile());
            log.info("Deduplication: Per-concept (during finalization) - check finalization logs for stats");
        }
        } finally {
            // Clean up MDC
            MDC.clear();
        }
    }

    /**
     * Check for deprecated CLI arguments and log warnings.
     */
    private void checkDeprecatedArgs(String[] args) {
        if (Arrays.stream(args).anyMatch(arg -> arg.startsWith("--parquet.dir="))) {
            log.atWarn()
                .addKeyValue("event_type", "config.deprecated.arg")
                .addKeyValue("event_schema_version", "1.0")
                .addKeyValue("deprecated_arg", "--parquet.dir")
                .addKeyValue("replacement_arg", "--ingest.parquet.base-dir")
                .log("DEPRECATED: --parquet.dir is deprecated, use --ingest.parquet.base-dir");
        }
        if (Arrays.stream(args).anyMatch(arg -> arg.startsWith("--parquet.config="))) {
            log.atWarn()
                .addKeyValue("event_type", "config.deprecated.arg")
                .addKeyValue("event_schema_version", "1.0")
                .addKeyValue("deprecated_arg", "--parquet.config")
                .addKeyValue("replacement_arg", "--ingest.parquet.config-path")
                .log("DEPRECATED: --parquet.config is deprecated, use --ingest.parquet.config-path");
        }
        if (Arrays.stream(args).anyMatch(arg -> arg.startsWith("--mapping.dbgapFile="))) {
            log.atWarn()
                .addKeyValue("event_type", "config.deprecated.arg")
                .addKeyValue("event_schema_version", "1.0")
                .addKeyValue("deprecated_arg", "--mapping.dbgapFile")
                .addKeyValue("replacement_arg", "--ingest.mapping.dbgap-file")
                .log("DEPRECATED: --mapping.dbgapFile is deprecated, use --ingest.mapping.dbgap-file");
        }
        if (Arrays.stream(args).anyMatch(arg -> arg.startsWith("--mapping.patientMappingFile="))) {
            log.atWarn()
                .addKeyValue("event_type", "config.deprecated.arg")
                .addKeyValue("event_schema_version", "1.0")
                .addKeyValue("deprecated_arg", "--mapping.patientMappingFile")
                .addKeyValue("replacement_arg", "--ingest.mapping.patient-file")
                .log("DEPRECATED: --mapping.patientMappingFile is deprecated, use --ingest.mapping.patient-file");
        }
        if (Arrays.stream(args).anyMatch(arg -> arg.startsWith("--output.dir="))) {
            log.atWarn()
                .addKeyValue("event_type", "config.deprecated.arg")
                .addKeyValue("event_schema_version", "1.0")
                .addKeyValue("deprecated_arg", "--output.dir")
                .addKeyValue("replacement_arg", "--ingest.output.dir")
                .log("DEPRECATED: --output.dir is deprecated, use --ingest.output.dir");
        }
    }

    /**
     * Initialize patient ID resolver and optionally inspect mapping files.
     */
    private PatientIdResolver initializePatientIdResolver(String telemetryReportFile, String patientMappingFile, boolean inspectOnly) throws IOException {
        Path telemetryReportPath = Path.of(telemetryReportFile);
        Path patientMappingPath = Path.of(patientMappingFile);

        // Inspect mapping files if requested
        if (inspectOnly) {
            log.info("=== MAPPING FILE INSPECTION MODE ===");

            MappingFileInspector.InspectionResult telemetryInspection = MappingFileInspector.inspect(telemetryReportPath);
            MappingFileInspector.printInspection(telemetryInspection);

            MappingFileInspector.InspectionResult patientInspection = MappingFileInspector.inspect(patientMappingPath);
            MappingFileInspector.printInspection(patientInspection);

            return null; // Exit after inspection
        }

        // Build generic mapping chain resolver
        log.info("Initializing mapping chain patient ID resolver...");

        // Step 1 config: SUBJECT_ID -> dbgap_subject_id (TSV with header)
        DelimitedFileMappingLoader.LoaderConfig step1Config =
            DelimitedFileMappingLoader.LoaderConfig.withHeader(
                telemetryReportPath,
                '\t',
                "SUBJECT_ID",
                "dbgap_subject_id"
            );

        // Step 2 config: dbgap_subject_id -> patient_num (CSV without header, columns 0 and 2)
        DelimitedFileMappingLoader.LoaderConfig step2Config =
            DelimitedFileMappingLoader.LoaderConfig.withoutHeader(
                patientMappingPath,
                ',',
                0,  // dbgap_subject_id in column 0
                2   // patient_num in column 2
            );

        MappingChainPatientIdResolver.ChainConfig chainConfig =
            new MappingChainPatientIdResolver.ChainConfig(
                step1Config,
                step2Config,
                "participantId -> dbgap_subject_id",
                "dbgap_subject_id -> patientNum"
            );

        PatientIdResolver resolver = new MappingChainPatientIdResolver(chainConfig);

        PatientIdResolver.MappingStatistics stats = resolver.getStatistics();
        log.info("Loaded mapping chain:");
        log.info("  Step 1 mappings: {} (from {})", stats.dbgapMappingCount(), stats.dbgapMappingFile());
        log.info("  Step 2 mappings: {} (from {})", stats.patientMappingCount(), stats.patientMappingFile());

        return resolver;
    }

    /**
     * Processes all Parquet datasets from config.
     * Config path can be either:
     * - A single JSONL file (one config per line)
     * - A directory containing JSONL files
     *
     * @return List of loaded dataset configs (for validation)
     */
    private List<ParquetDatasetConfig> processParquetDatasets(String runId, String baseDir, String configPath,
                                       FailureSink failureSink, HPDSWriterAdapter writer,
                                       AtomicLong totalObservations, PatientIdResolver patientIdResolver) throws IOException {
        ObjectMapper mapper = new ObjectMapper();

        // Load configs from file or directory
        List<ParquetDatasetConfig> configs = loadDatasetConfigs(configPath, mapper);
        log.info("Loaded {} Parquet dataset configs", configs.size());

        for (ParquetDatasetConfig config : configs) {
            log.info("Processing dataset: {}", config.datasetName());
            if ("none".equalsIgnoreCase(config.timestampColumn())) {
                log.info("Processing dataset without timestamps (relational/demographic data): {}", config.datasetName());
            }

            ParquetObservationProducer producer = new ParquetObservationProducer(runId, config, failureSink, patientIdResolver);

            // Set per-file observation limit (use dataset config or global default)
            long perFileLimit = config.maxObservationsPerFile() != null
                ? config.maxObservationsPerFile()
                : this.config.getMaxObservationsPerFile(); // Global default from IngestConfig

            producer.setPerFileObservationLimit(perFileLimit);
            log.info("Dataset '{}': per-file observation limit = {}", config.datasetName(), perFileLimit);

            // Find all parquet files for this dataset
            Path datasetPath = Paths.get(baseDir, config.datasetName());
            if (!Files.exists(datasetPath)) {
                log.warn("Dataset directory not found: {}", datasetPath);
                continue;
            }

            // Collect all Parquet files for parallel processing
            List<Path> fileList;
            try (Stream<Path> files = Files.walk(datasetPath)) {
                fileList = files.filter(p -> p.toString().endsWith(".parquet"))
                               .collect(Collectors.toList());
            }

            if (fileList.isEmpty()) {
                log.warn("No Parquet files found in dataset: {}", config.datasetName());
                continue;
            }

            log.info("Found {} Parquet files for dataset: {}", fileList.size(), config.datasetName());

            // Process files in parallel using ExecutorService
            processFilesInParallel(fileList, producer, writer, totalObservations);
        }

        return configs;
    }

    /**
     * Processes files in parallel using ExecutorService with progress reporting.
     */
    private void processFilesInParallel(List<Path> fileList, ParquetObservationProducer producer,
                                       HPDSWriterAdapter writer, AtomicLong totalObservations) throws IOException {
        // Use virtual threads with bounded concurrency to prevent OOM from Arrow off-heap allocations
        // Each file needs ~100MB of direct memory for Arrow buffers, so we limit concurrency
        // based on available MaxDirectMemorySize
        ExecutorService fileProcessorPool = Executors.newFixedThreadPool(
            config.getFileProcessingThreads(),
            Thread.ofVirtual().factory()
        );

        log.info("Processing {} files with {} bounded virtual threads", fileList.size(), config.getFileProcessingThreads());

        List<Future<ProcessingResult>> futures = new ArrayList<>();
        for (Path file : fileList) {
            Future<ProcessingResult> future = fileProcessorPool.submit(() -> {
                try {
                    AtomicInteger batchAccepted = new AtomicInteger(0);
                    String sourceName = "parquet:" + producer.getDatasetName() + ":" + file.getFileName();
                    producer.processFile(file, batch -> {
                        int accepted = writer.acceptBatch(batch, sourceName);
                        batchAccepted.addAndGet(accepted);
                    }, config.getBatchSize());
                    return new ProcessingResult(file, batchAccepted.get(), null);
                } catch (Exception e) {
                    return new ProcessingResult(file, 0, e);
                }
            });
            futures.add(future);
        }

        // Collect results with progress reporting
        int completed = 0;
        int failed = 0;
        for (Future<ProcessingResult> future : futures) {
            try {
                ProcessingResult result = future.get();
                completed++;

                if (result.error != null) {
                    failed++;
                    log.error("File processing failed: {}", result.file, result.error);
                } else {
                    totalObservations.addAndGet(result.observationCount);
                }

                // Progress logging every 100 files
                if (completed % 100 == 0) {
                    log.info("Progress: {}/{} files completed ({} failed)", completed, fileList.size(), failed);
                }
            } catch (InterruptedException | ExecutionException e) {
                log.error("Failed to retrieve file processing result", e);
                failed++;
            }
        }

        fileProcessorPool.shutdown();
        try {
            if (!fileProcessorPool.awaitTermination(1, TimeUnit.HOURS)) {
                log.warn("File processor pool did not terminate within 1 hour, forcing shutdown");
                fileProcessorPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.error("Interrupted while waiting for file processor pool to terminate", e);
            fileProcessorPool.shutdownNow();
            Thread.currentThread().interrupt();
        }

        log.info("Completed processing {} files ({} failed)", fileList.size(), failed);
    }

    /**
     * Result of processing a single Parquet file.
     */
    private static class ProcessingResult {
        final Path file;
        final int observationCount;
        final Exception error;

        ProcessingResult(Path file, int observationCount, Exception error) {
            this.file = file;
            this.observationCount = observationCount;
            this.error = error;
        }
    }

    /**
     * Processes all CSV files in parallel.
     */
    private void processCsvFiles(String runId, String csvDir, FailureSink failureSink,
                                HPDSWriterAdapter writer, AtomicLong totalObservations) throws IOException {
        CsvObservationProducer producer = new CsvObservationProducer(runId, failureSink);

        // Collect all CSV files
        List<Path> fileList;
        try (Stream<Path> files = Files.walk(Path.of(csvDir))) {
            fileList = files.filter(p -> p.toString().endsWith(".csv"))
                           .sorted() // Deterministic order
                           .collect(Collectors.toList());
        }

        if (fileList.isEmpty()) {
            log.warn("No CSV files found in directory: {}", csvDir);
            return;
        }

        log.info("Found {} CSV files", fileList.size());

        // Process files in parallel using ExecutorService (same pattern as Parquet)
        processCsvFilesInParallel(fileList, producer, writer, totalObservations);
    }

    /**
     * Processes CSV files in parallel using ExecutorService with progress reporting.
     */
    private void processCsvFilesInParallel(List<Path> fileList, CsvObservationProducer producer,
                                          HPDSWriterAdapter writer, AtomicLong totalObservations) throws IOException {
        // Use virtual threads with bounded concurrency to prevent OOM from Arrow off-heap allocations
        ExecutorService fileProcessorPool = Executors.newFixedThreadPool(
            config.getFileProcessingThreads(),
            Thread.ofVirtual().factory()
        );

        log.info("Processing {} CSV files with {} bounded virtual threads", fileList.size(), config.getFileProcessingThreads());

        List<Future<ProcessingResult>> futures = new ArrayList<>();
        for (Path file : fileList) {
            Future<ProcessingResult> future = fileProcessorPool.submit(() -> {
                try {
                    log.info("Processing CSV file: {} on thread: {}", file.getFileName(), Thread.currentThread().getName());
                    AtomicInteger batchAccepted = new AtomicInteger(0);
                    String sourceName = "csv:" + file.getFileName();
                    producer.processFile(file, batch -> {
                        int accepted = writer.acceptBatch(batch, sourceName);
                        batchAccepted.addAndGet(accepted);
                    }, config.getBatchSize());
                    return new ProcessingResult(file, batchAccepted.get(), null);
                } catch (Exception e) {
                    return new ProcessingResult(file, 0, e);
                }
            });
            futures.add(future);
        }

        // Collect results with progress reporting
        int completed = 0;
        int failed = 0;
        for (Future<ProcessingResult> future : futures) {
            try {
                ProcessingResult result = future.get();
                completed++;

                if (result.error != null) {
                    failed++;
                    log.error("CSV file processing failed: {}", result.file, result.error);
                } else {
                    totalObservations.addAndGet(result.observationCount);
                    log.info("CSV file completed: {} ({} observations)", result.file.getFileName(), result.observationCount);
                }

                // Progress logging every 10 files (CSV files are typically fewer and larger than Parquet)
                if (completed % 10 == 0) {
                    log.info("CSV Progress: {}/{} files completed ({} failed)", completed, fileList.size(), failed);
                }
            } catch (InterruptedException | ExecutionException e) {
                log.error("Failed to retrieve CSV file processing result", e);
                failed++;
            }
        }

        fileProcessorPool.shutdown();
        try {
            if (!fileProcessorPool.awaitTermination(1, TimeUnit.HOURS)) {
                log.warn("CSV file processor pool did not terminate within 1 hour, forcing shutdown");
                fileProcessorPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.error("Interrupted while waiting for CSV file processor pool to terminate", e);
            fileProcessorPool.shutdownNow();
            Thread.currentThread().interrupt();
        }

        log.info("Completed processing {} CSV files ({} failed)", fileList.size(), failed);
    }

    /**
     * Load dataset configs from file or directory.
     *
     * If configPath is a file: read JSONL (one config per line)
     * If configPath is a directory: read all .jsonl files in directory
     */
    private List<ParquetDatasetConfig> loadDatasetConfigs(String configPath, ObjectMapper mapper) throws IOException {
        Path path = Path.of(configPath);
        List<ParquetDatasetConfig> configs = new java.util.ArrayList<>();

        if (Files.isDirectory(path)) {
            log.info("Loading dataset configs from directory: {}", configPath);
            try (Stream<Path> files = Files.walk(path, 1)) {
                files.filter(p -> Files.isRegularFile(p) && p.toString().endsWith(".jsonl"))
                    .sorted()
                    .forEach(file -> {
                        try {
                            log.info("Reading config file: {}", file.getFileName());
                            configs.addAll(loadConfigsFromFile(file, mapper));
                        } catch (IOException e) {
                            log.error("Failed to load config file: {}", file, e);
                        }
                    });
            }
        } else if (Files.isRegularFile(path)) {
            log.info("Loading dataset configs from file: {}", configPath);
            configs.addAll(loadConfigsFromFile(path, mapper));
        } else {
            throw new IOException("Config path not found or invalid: " + configPath);
        }

        return configs;
    }

    /**
     * Load configs from a single JSONL file (one config per line).
     */
    private List<ParquetDatasetConfig> loadConfigsFromFile(Path file, ObjectMapper mapper) throws IOException {
        return Files.readAllLines(file).stream()
            .filter(line -> !line.isBlank())
            .map(line -> {
                try {
                    return mapper.readValue(line, ParquetDatasetConfig.class);
                } catch (Exception e) {
                    log.error("Failed to parse config line in {}: {}", file.getFileName(), line, e);
                    return null;
                }
            })
            .filter(c -> c != null)
            .toList();
    }

    /**
     * Generate and save encryption key for HPDS.
     *
     * The key is saved as a 32-character string (32 bytes when UTF-8 encoded).
     */
    private void generateAndSaveEncryptionKey(String keyName, String keyFilePath) throws IOException {
        // Generate a random 32-character string (will be 32 bytes when read back)
        java.security.SecureRandom random = new java.security.SecureRandom();
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*";
        StringBuilder key = new StringBuilder(32);
        for (int i = 0; i < 32; i++) {
            key.append(chars.charAt(random.nextInt(chars.length())));
        }

        // Save to file (32 characters = 32 bytes when read as UTF-8)
        Path keyPath = Path.of(keyFilePath);
        Files.createDirectories(keyPath.getParent());
        Files.writeString(keyPath, key.toString());

        // Load into Crypto
        edu.harvard.hms.dbmi.avillach.hpds.crypto.Crypto.loadKey(keyName, keyFilePath);

        log.warn("=== GENERATED NEW ENCRYPTION KEY ===");
        log.warn("Key saved to: {}", keyFilePath);
        log.warn("IMPORTANT: Save this key securely for decryption!");
        log.warn("====================================");
    }
}

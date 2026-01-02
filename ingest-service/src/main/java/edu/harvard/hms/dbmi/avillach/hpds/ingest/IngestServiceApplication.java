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
        log.info("Starting HPDS multi-source ingestion");

        // Check for deprecated CLI args and warn
        checkDeprecatedArgs(args);

        String runId = UUID.randomUUID().toString();
        log.info("Run ID: {}", runId);

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
                 config.getMaxObservationsPerConcept())) {

            // Process Parquet datasets
            log.info("Processing Parquet datasets from config: {}", config.getParquetConfigPath());
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
                log.error("!!! NO DATASET CONFIGS FOUND - ZERO INGESTION !!!");
                log.error("Config path: {}", config.getParquetConfigPath());
            }

            // Process CSV files (optional)
            if (config.getCsvDir() != null) {
                log.info("Processing CSV files from: {}", config.getCsvDir());
                processCsvFiles(runId, config.getCsvDir(), failureSink, writer, totalObservations);
            }

            // Finalize stores
            log.info("Finalizing HPDS stores...");
            writer.closeAndSave();

            Instant endTime = Instant.now();
            long durationSeconds = endTime.getEpochSecond() - startTime.getEpochSecond();

            // Log completion with warning if zero ingestion
            if (totalObservations.get() == 0 && failureSink.getTotalFailures() == 0) {
                log.error("=== INGESTION COMPLETE (BUT WITH ZERO OBSERVATIONS) ===");
                log.error("This usually indicates a configuration problem:");
                log.error("  - Config path may not contain any .jsonl files");
                log.error("  - Dataset names in config may not match directory names");
                log.error("  - Parquet base directory may be incorrect");
            } else {
                log.info("=== INGESTION COMPLETE ===");
            }

            log.info("Run ID: {}", runId);
            log.info("Total observations: {}", totalObservations.get());
            log.info("Total patients: {}", writer.getPatientCount());
            log.info("Total failures: {}", failureSink.getTotalFailures());
            log.info("Duration: {} seconds", durationSeconds);
            log.info("Output: {}", config.getOutputDir());
            log.info("Failures: {}", config.getFailureFile());
        }
    }

    /**
     * Check for deprecated CLI arguments and log warnings.
     */
    private void checkDeprecatedArgs(String[] args) {
        if (Arrays.stream(args).anyMatch(arg -> arg.startsWith("--parquet.dir="))) {
            log.warn("DEPRECATED: --parquet.dir is deprecated, use --ingest.parquet.base-dir");
        }
        if (Arrays.stream(args).anyMatch(arg -> arg.startsWith("--parquet.config="))) {
            log.warn("DEPRECATED: --parquet.config is deprecated, use --ingest.parquet.config-path");
        }
        if (Arrays.stream(args).anyMatch(arg -> arg.startsWith("--mapping.dbgapFile="))) {
            log.warn("DEPRECATED: --mapping.dbgapFile is deprecated, use --ingest.mapping.dbgap-file");
        }
        if (Arrays.stream(args).anyMatch(arg -> arg.startsWith("--mapping.patientMappingFile="))) {
            log.warn("DEPRECATED: --mapping.patientMappingFile is deprecated, use --ingest.mapping.patient-file");
        }
        if (Arrays.stream(args).anyMatch(arg -> arg.startsWith("--output.dir="))) {
            log.warn("DEPRECATED: --output.dir is deprecated, use --ingest.output.dir");
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
        int threadCount = config.getFileProcessingThreads();
        ExecutorService fileProcessorPool = Executors.newFixedThreadPool(threadCount);

        log.info("Processing {} files with {} threads", fileList.size(), threadCount);

        List<Future<ProcessingResult>> futures = new ArrayList<>();
        for (Path file : fileList) {
            Future<ProcessingResult> future = fileProcessorPool.submit(() -> {
                try {
                    AtomicInteger batchAccepted = new AtomicInteger(0);
                    producer.processFile(file, batch -> {
                        int accepted = writer.acceptBatch(batch);
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
     * Processes all CSV files.
     */
    private void processCsvFiles(String runId, String csvDir, FailureSink failureSink,
                                HPDSWriterAdapter writer, AtomicLong totalObservations) throws IOException {
        CsvObservationProducer producer = new CsvObservationProducer(runId, failureSink);

        try (Stream<Path> files = Files.walk(Path.of(csvDir))) {
            files.filter(p -> p.toString().endsWith(".csv"))
                .sorted() // Deterministic order
                .forEach(file -> {
                    try {
                        log.info("Processing: {}", file);
                        producer.processFile(file, batch -> {
                            int accepted = writer.acceptBatch(batch);
                            totalObservations.addAndGet(accepted);
                        }, config.getBatchSize());
                    } catch (IOException e) {
                        log.error("Failed to process file: {}", file, e);
                    }
                });
        }
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

package edu.harvard.hms.dbmi.avillach.hpds.ingest.config;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Configuration properties for HPDS ingestion service.
 *
 * Uses Spring Boot property binding with fail-fast validation.
 * All properties use the "ingest.*" prefix.
 */
@ConfigurationProperties(prefix = "ingest")
@Validated
public class IngestConfig {
    private static final Logger log = LoggerFactory.getLogger(IngestConfig.class);

    // Required properties
    private String parquetBaseDir;
    private String parquetConfigPath;
    private String mappingTelemetryReportFile;
    private String mappingPatientMappingFile;
    private String outputDir;

    // Optional properties with defaults
    private String spoolDir;
    private String failureFile;
    private String csvDir; // null = skip CSV processing
    private String encryptionKeyName = "encryption_key";
    private boolean mappingInspectOnly = false;
    private int batchSize = 10000;
    private int storeCacheSize = 16;
    private int fileProcessingThreads = 12; // Default for r7g.4xlarge (16 vCPU - 4 overhead)

    @PostConstruct
    public void validateAndLog() {
        log.info("=== VALIDATING CONFIGURATION ===");

        // Collect all validation errors
        List<String> errors = new ArrayList<>();

        // Check required properties are set
        if (parquetBaseDir == null || parquetBaseDir.isBlank()) {
            errors.add("ingest.parquet.base-dir is required");
        }
        if (parquetConfigPath == null || parquetConfigPath.isBlank()) {
            errors.add("ingest.parquet.config-path is required");
        }
        if (mappingTelemetryReportFile == null || mappingTelemetryReportFile.isBlank()) {
            errors.add("ingest.mapping-telemetry-report-file is required");
        }
        if (mappingPatientMappingFile == null || mappingPatientMappingFile.isBlank()) {
            errors.add("ingest.mapping-patient-mapping-file is required");
        }
        if (outputDir == null || outputDir.isBlank()) {
            errors.add("ingest.output.dir is required");
        }

        // If any required properties missing, fail immediately
        if (!errors.isEmpty()) {
            String errorMsg = "Missing required configuration properties:\n  - " + String.join("\n  - ", errors);
            log.error(errorMsg);
            throw new IllegalStateException(errorMsg);
        }

        // Now validate paths exist
        errors.clear();

        Path parquetPath = Path.of(parquetBaseDir);
        if (!Files.exists(parquetPath)) {
            errors.add("Parquet base directory not found: " + parquetBaseDir);
        } else if (!Files.isDirectory(parquetPath)) {
            errors.add("Parquet base path is not a directory: " + parquetBaseDir);
        }

        Path configPath = Path.of(parquetConfigPath);
        if (!Files.exists(configPath)) {
            errors.add("Config path not found: " + parquetConfigPath);
        }

        Path telemetryReportPath = Path.of(mappingTelemetryReportFile);
        if (!Files.exists(telemetryReportPath)) {
            errors.add("Telemetry report file not found: " + mappingTelemetryReportFile);
        }

        Path patientMappingPath = Path.of(mappingPatientMappingFile);
        if (!Files.exists(patientMappingPath)) {
            errors.add("Patient mapping file not found: " + mappingPatientMappingFile);
        }

        // Check output directory is writable (create if needed)
        Path outputPath = Path.of(outputDir);
        try {
            Files.createDirectories(outputPath);
            if (!Files.isWritable(outputPath)) {
                errors.add("Output directory is not writable: " + outputDir);
            }
        } catch (Exception e) {
            errors.add("Cannot create output directory: " + outputDir + " - " + e.getMessage());
        }

        // Set defaults for optional properties
        if (spoolDir == null || spoolDir.isBlank()) {
            spoolDir = outputDir + "/spool";
        }
        if (failureFile == null || failureFile.isBlank()) {
            failureFile = outputDir + "/failures.jsonl";
        }

        // Fail fast if any errors
        if (!errors.isEmpty()) {
            String errorMsg = "Configuration validation failed:\n  - " + String.join("\n  - ", errors);
            log.error(errorMsg);
            throw new IllegalStateException(errorMsg);
        }

        // Log effective configuration
        log.info("=== EFFECTIVE CONFIGURATION ===");
        log.info("Parquet base dir: {}", parquetBaseDir);
        log.info("Parquet config: {}", parquetConfigPath);
        log.info("Output dir: {}", outputDir);
        log.info("Spool dir: {}", spoolDir);
        log.info("Failure file: {}", failureFile);
        log.info("Mapping files: telemetryReport={}, patientMapping={}", mappingTelemetryReportFile, mappingPatientMappingFile);
        log.info("CSV dir: {}", csvDir != null ? csvDir : "(none - CSV processing disabled)");
        log.info("Encryption key name: {}", encryptionKeyName);
        log.info("Mapping inspect only: {}", mappingInspectOnly);
        log.info("Batch size: {}", batchSize);
        log.info("File processing threads: {}", fileProcessingThreads);
        log.info("================================");
    }

    public String getParquetBaseDir() {
        return parquetBaseDir;
    }

    public void setParquetBaseDir(String parquetBaseDir) {
        this.parquetBaseDir = parquetBaseDir;
    }

    public String getParquetConfigPath() {
        return parquetConfigPath;
    }

    public void setParquetConfigPath(String parquetConfigPath) {
        this.parquetConfigPath = parquetConfigPath;
    }

    public String getMappingTelemetryReportFile() {
        return mappingTelemetryReportFile;
    }

    public void setMappingTelemetryReportFile(String mappingTelemetryReportFile) {
        this.mappingTelemetryReportFile = mappingTelemetryReportFile;
    }

    public String getMappingPatientMappingFile() {
        return mappingPatientMappingFile;
    }

    public void setMappingPatientMappingFile(String mappingPatientMappingFile) {
        this.mappingPatientMappingFile = mappingPatientMappingFile;
    }

    public String getOutputDir() {
        return outputDir;
    }

    public void setOutputDir(String outputDir) {
        this.outputDir = outputDir;
    }

    public String getSpoolDir() {
        return spoolDir;
    }

    public void setSpoolDir(String spoolDir) {
        this.spoolDir = spoolDir;
    }

    public String getFailureFile() {
        return failureFile;
    }

    public void setFailureFile(String failureFile) {
        this.failureFile = failureFile;
    }

    public String getCsvDir() {
        return csvDir;
    }

    public void setCsvDir(String csvDir) {
        this.csvDir = csvDir;
    }

    public String getEncryptionKeyName() {
        return encryptionKeyName;
    }

    public void setEncryptionKeyName(String encryptionKeyName) {
        this.encryptionKeyName = encryptionKeyName;
    }

    public boolean isMappingInspectOnly() {
        return mappingInspectOnly;
    }

    public void setMappingInspectOnly(boolean mappingInspectOnly) {
        this.mappingInspectOnly = mappingInspectOnly;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getStoreCacheSize() {
        return storeCacheSize;
    }

    public void setStoreCacheSize(int storeCacheSize) {
        this.storeCacheSize = storeCacheSize;
    }

    public int getFileProcessingThreads() {
        return fileProcessingThreads;
    }

    public void setFileProcessingThreads(int fileProcessingThreads) {
        this.fileProcessingThreads = fileProcessingThreads;
    }
}

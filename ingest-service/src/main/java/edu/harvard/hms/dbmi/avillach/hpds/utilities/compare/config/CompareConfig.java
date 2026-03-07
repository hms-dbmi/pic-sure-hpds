package edu.harvard.hms.dbmi.avillach.hpds.utilities.compare.config;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Configuration properties for ColumnMeta comparison utility.
 * Uses Spring Boot property binding with fail-fast validation.
 * All properties use the "compare.*" prefix.
 */
@Component
@ConfigurationProperties(prefix = "compare")
@Validated
public class CompareConfig {
    private static final Logger log = LoggerFactory.getLogger(CompareConfig.class);

    // Required properties
    private String fileA;
    private String fileB;

    // Optional properties with defaults
    private String outputDir = ".";

    @PostConstruct
    public void validateAndLog() {
        log.info("=== COMPARE CONFIGURATION ===");

        // Validate file A
        if (fileA == null || fileA.isBlank()) {
            throw new IllegalStateException("compare.file-a is required");
        }
        Path pathA = Paths.get(fileA);
        if (!Files.exists(pathA)) {
            throw new IllegalStateException("compare.file-a does not exist: " + fileA);
        }
        if (!Files.isRegularFile(pathA)) {
            throw new IllegalStateException("compare.file-a is not a regular file: " + fileA);
        }
        log.info("File A: {}", pathA.toAbsolutePath());

        // Validate file B
        if (fileB == null || fileB.isBlank()) {
            throw new IllegalStateException("compare.file-b is required");
        }
        Path pathB = Paths.get(fileB);
        if (!Files.exists(pathB)) {
            throw new IllegalStateException("compare.file-b does not exist: " + fileB);
        }
        if (!Files.isRegularFile(pathB)) {
            throw new IllegalStateException("compare.file-b is not a regular file: " + fileB);
        }
        log.info("File B: {}", pathB.toAbsolutePath());

        // Validate output directory
        Path outputPath = Paths.get(outputDir);
        log.info("Output directory: {}", outputPath.toAbsolutePath());

        log.info("=== CONFIGURATION VALID ===");
    }

    public String getFileA() {
        return fileA;
    }

    public void setFileA(String fileA) {
        this.fileA = fileA;
    }

    public String getFileB() {
        return fileB;
    }

    public void setFileB(String fileB) {
        this.fileB = fileB;
    }

    public String getOutputDir() {
        return outputDir;
    }

    public void setOutputDir(String outputDir) {
        this.outputDir = outputDir;
    }
}

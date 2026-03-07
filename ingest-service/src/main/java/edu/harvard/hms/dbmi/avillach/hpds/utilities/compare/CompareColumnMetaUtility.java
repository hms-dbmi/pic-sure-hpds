package edu.harvard.hms.dbmi.avillach.hpds.utilities.compare;

import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.ColumnMeta;
import edu.harvard.hms.dbmi.avillach.hpds.utilities.compare.config.CompareConfig;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.*;
import java.util.zip.GZIPInputStream;

/**
 * Spring-based utility to compare two columnMeta.javabin files and generate detailed comparison reports.
 *
 * <p>Generates 8 detailed reports:
 * <ul>
 *   <li>comparison_summary.txt - High-level statistics</li>
 *   <li>matching_identical.csv - Concepts with identical metadata</li>
 *   <li>matching_type_changed.csv - Type conversions (categorical ↔ numeric)</li>
 *   <li>matching_counts_changed.csv - Observation/patient count changes</li>
 *   <li>matching_categories_changed.csv - Category value changes</li>
 *   <li>matching_minmax_changed.csv - Min/max value changes</li>
 *   <li>missing_in_b.csv - Concepts in A but not in B</li>
 *   <li>missing_in_a.csv - Concepts in B but not in A</li>
 * </ul>
 *
 * <p>Usage via Spring Boot application:
 * <pre>
 * java -jar ingest-service.jar \
 *   --spring.main.banner-mode=off \
 *   --compare.file-a=/path/to/A/columnMeta.javabin \
 *   --compare.file-b=/path/to/B/columnMeta.javabin \
 *   --compare.output-dir=/path/to/output
 * </pre>
 *
 * <p>Or use the wrapper script:
 * <pre>
 * ./bin/compare-columnmeta.sh \
 *   /path/to/A/columnMeta.javabin \
 *   /path/to/B/columnMeta.javabin \
 *   /path/to/output
 * </pre>
 */
@Component
public class CompareColumnMetaUtility {
    private static final Logger log = LoggerFactory.getLogger(CompareColumnMetaUtility.class);

    private final CompareConfig config;

    public CompareColumnMetaUtility(CompareConfig config) {
        this.config = config;
    }

    /**
     * Perform the comparison and generate reports.
     */
    public void compare() throws IOException {
        Instant startTime = Instant.now();

        // Load both files
        log.info("Loading file A...");
        Map<String, ColumnMeta> metaA = loadColumnMeta(Paths.get(config.getFileA()));
        log.info("Loaded {} concepts from file A", metaA.size());

        log.info("Loading file B...");
        Map<String, ColumnMeta> metaB = loadColumnMeta(Paths.get(config.getFileB()));
        log.info("Loaded {} concepts from file B", metaB.size());

        // Perform comparison
        log.info("Comparing metadata...");
        ComparisonResult result = compareMetadata(metaA, metaB);
        result.fileAPath = config.getFileA();
        result.fileBPath = config.getFileB();

        // Ensure output directory exists
        Path outputPath = Paths.get(config.getOutputDir());
        Files.createDirectories(outputPath);

        // Write reports
        log.info("Writing reports to: {}", outputPath.toAbsolutePath());
        writeReports(result, outputPath);

        Instant endTime = Instant.now();
        long durationSeconds = endTime.getEpochSecond() - startTime.getEpochSecond();

        log.info("=== Comparison Complete ({} seconds) ===", durationSeconds);
        log.info("Reports written to: {}", outputPath.toAbsolutePath());
    }

    /**
     * Load columnMeta.javabin file and deserialize to Map.
     */
    @SuppressWarnings("unchecked")
    private Map<String, ColumnMeta> loadColumnMeta(Path path) throws IOException {
        try (ObjectInputStream ois = new ObjectInputStream(
                new GZIPInputStream(new FileInputStream(path.toFile())))) {
            Object obj = ois.readObject();
            if (!(obj instanceof Map)) {
                throw new IOException("Expected Map<String, ColumnMeta>, got: " + obj.getClass());
            }
            return (Map<String, ColumnMeta>) obj;
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize columnMeta.javabin", e);
        }
    }

    /**
     * Compare two columnMeta maps and categorize differences.
     */
    private ComparisonResult compareMetadata(Map<String, ColumnMeta> metaA, Map<String, ColumnMeta> metaB) {
        ComparisonResult result = new ComparisonResult();
        result.metaA = metaA;
        result.metaB = metaB;
        result.fileASize = getSizeInBytes(config.getFileA());
        result.fileBSize = getSizeInBytes(config.getFileB());

        Set<String> keysA = metaA.keySet();
        Set<String> keysB = metaB.keySet();

        // Compute set differences
        result.matchingKeys = new TreeSet<>(keysA);
        result.matchingKeys.retainAll(keysB);

        result.missingInB = new TreeSet<>(keysA);
        result.missingInB.removeAll(keysB);

        result.missingInA = new TreeSet<>(keysB);
        result.missingInA.removeAll(keysA);

        log.info("Matching keys: {}", result.matchingKeys.size());
        log.info("Missing in B: {}", result.missingInB.size());
        log.info("Missing in A: {}", result.missingInA.size());

        // Analyze matching concepts for metadata changes
        for (String conceptPath : result.matchingKeys) {
            ColumnMeta a = metaA.get(conceptPath);
            ColumnMeta b = metaB.get(conceptPath);

            MatchedConcept matched = new MatchedConcept(conceptPath, a, b);

            if (matched.isIdentical()) {
                result.identical.add(matched);
            } else {
                if (matched.isTypeChanged()) {
                    result.typeChanged.add(matched);
                }
                if (matched.isCountsChanged()) {
                    result.countsChanged.add(matched);
                }
                if (matched.isCategoriesChanged()) {
                    result.categoriesChanged.add(matched);
                }
                if (matched.isMinMaxChanged()) {
                    result.minMaxChanged.add(matched);
                }
            }
        }

        log.info("  Identical metadata: {}", result.identical.size());
        log.info("  Type changed: {}", result.typeChanged.size());
        log.info("  Counts changed: {}", result.countsChanged.size());
        log.info("  Categories changed: {}", result.categoriesChanged.size());
        log.info("  Min/Max changed: {}", result.minMaxChanged.size());

        return result;
    }

    /**
     * Write all comparison reports.
     */
    private void writeReports(ComparisonResult result, Path outputDir) throws IOException {
        writeSummary(result, outputDir.resolve("comparison_summary.txt"));
        writeMatchingIdentical(result, outputDir.resolve("matching_identical.csv"));
        writeMatchingTypeChanged(result, outputDir.resolve("matching_type_changed.csv"));
        writeMatchingCountsChanged(result, outputDir.resolve("matching_counts_changed.csv"));
        writeMatchingCategoriesChanged(result, outputDir.resolve("matching_categories_changed.csv"));
        writeMatchingMinMaxChanged(result, outputDir.resolve("matching_minmax_changed.csv"));
        writeMissingInB(result, outputDir.resolve("missing_in_b.csv"));
        writeMissingInA(result, outputDir.resolve("missing_in_a.csv"));

        log.info("✓ All reports written successfully");
    }

    /**
     * Write summary text file.
     */
    private void writeSummary(ComparisonResult result, Path path) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(path,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {

            writer.write("=== ColumnMeta Comparison Summary ===\n");
            writer.write("Timestamp: " + Instant.now() + "\n");
            writer.write("File A: " + result.fileAPath + String.format(" (%.1f MB, %,d concepts)\n",
                    result.fileASize / 1_048_576.0, result.metaA.size()));
            writer.write("File B: " + result.fileBPath + String.format(" (%.1f MB, %,d concepts)\n\n",
                    result.fileBSize / 1_048_576.0, result.metaB.size()));

            writer.write("=== KEY COMPARISON ===\n");
            writer.write(String.format("Total concepts in A: %,d\n", result.metaA.size()));
            writer.write(String.format("Total concepts in B: %,d\n\n", result.metaB.size()));

            double matchPctA = 100.0 * result.matchingKeys.size() / result.metaA.size();
            double matchPctB = 100.0 * result.matchingKeys.size() / result.metaB.size();
            writer.write(String.format("Matching keys: %,d (%.1f%% of A, %.1f%% of B)\n",
                    result.matchingKeys.size(), matchPctA, matchPctB));

            double missingInBPct = 100.0 * result.missingInB.size() / result.metaA.size();
            writer.write(String.format("Missing in B: %,d (%.1f%% of A)\n",
                    result.missingInB.size(), missingInBPct));

            double missingInAPct = 100.0 * result.missingInA.size() / result.metaB.size();
            writer.write(String.format("Missing in A: %,d (%.1f%% of B)\n\n",
                    result.missingInA.size(), missingInAPct));

            writer.write("=== METADATA CHANGES (Matching Concepts) ===\n");
            double identicalPct = 100.0 * result.identical.size() / result.matchingKeys.size();
            writer.write(String.format("Identical metadata: %,d (%.1f%%)\n",
                    result.identical.size(), identicalPct));

            if (!result.typeChanged.isEmpty()) {
                double typePct = 100.0 * result.typeChanged.size() / result.matchingKeys.size();
                writer.write(String.format("Type changes: %,d (%.1f%%)\n", result.typeChanged.size(), typePct));

                long catToNum = result.typeChanged.stream()
                        .filter(m -> m.metaA.isCategorical() && !m.metaB.isCategorical())
                        .count();
                long numToCat = result.typeChanged.stream()
                        .filter(m -> !m.metaA.isCategorical() && m.metaB.isCategorical())
                        .count();
                writer.write(String.format("  - Categorical → Numeric: %,d\n", catToNum));
                writer.write(String.format("  - Numeric → Categorical: %,d\n", numToCat));
            }

            if (!result.countsChanged.isEmpty()) {
                double countsPct = 100.0 * result.countsChanged.size() / result.matchingKeys.size();
                writer.write(String.format("Count changes: %,d (%.1f%%)\n", result.countsChanged.size(), countsPct));

                long obsChanged = result.countsChanged.stream()
                        .filter(m -> m.metaA.getObservationCount() != m.metaB.getObservationCount())
                        .count();
                long patChanged = result.countsChanged.stream()
                        .filter(m -> m.metaA.getPatientCount() != m.metaB.getPatientCount())
                        .count();
                writer.write(String.format("  - Observation count changed: %,d\n", obsChanged));
                writer.write(String.format("  - Patient count changed: %,d\n", patChanged));
            }

            if (!result.categoriesChanged.isEmpty()) {
                double catPct = 100.0 * result.categoriesChanged.size() / result.matchingKeys.size();
                writer.write(String.format("Category value changes: %,d (%.1f%%)\n",
                        result.categoriesChanged.size(), catPct));
            }

            if (!result.minMaxChanged.isEmpty()) {
                double mmPct = 100.0 * result.minMaxChanged.size() / result.matchingKeys.size();
                writer.write(String.format("Min/Max changes: %,d (%.1f%%)\n",
                        result.minMaxChanged.size(), mmPct));
            }

            writer.write("\n=== DETAILED REPORTS ===\n");
            writer.write("1. matching_identical.csv - Concepts with identical metadata\n");
            writer.write("2. matching_type_changed.csv - Type conversions (categorical ↔ numeric)\n");
            writer.write("3. matching_counts_changed.csv - Observation/patient count changes\n");
            writer.write("4. matching_categories_changed.csv - Category value changes\n");
            writer.write("5. matching_minmax_changed.csv - Min/max value changes\n");
            writer.write("6. missing_in_b.csv - Concepts in A but not in B\n");
            writer.write("7. missing_in_a.csv - Concepts in B but not in A\n");
        }

        log.info("✓ Wrote comparison_summary.txt");
    }

    /**
     * Write matching_identical.csv report.
     */
    private void writeMatchingIdentical(ComparisonResult result, Path path) throws IOException {
        CSVFormat format = CSVFormat.RFC4180.builder().setHeader(
                "conceptPath", "observationCount", "patientCount", "categorical", "min", "max"
        ).build();

        try (BufferedWriter writer = Files.newBufferedWriter(path,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
             CSVPrinter printer = new CSVPrinter(writer, format)) {

            for (MatchedConcept m : result.identical) {
                printer.printRecord(
                        m.conceptPath,
                        m.metaA.getObservationCount(),
                        m.metaA.getPatientCount(),
                        m.metaA.isCategorical(),
                        m.metaA.getMin(),
                        m.metaA.getMax()
                );
            }
        }

        log.info("✓ Wrote matching_identical.csv ({} rows)", result.identical.size());
    }

    /**
     * Write matching_type_changed.csv report.
     */
    private void writeMatchingTypeChanged(ComparisonResult result, Path path) throws IOException {
        CSVFormat format = CSVFormat.RFC4180.builder().setHeader(
                "conceptPath", "type_A", "type_B", "observationCount_A", "observationCount_B",
                "patientCount_A", "patientCount_B", "categoryCount_A", "categoryCount_B",
                "min_A", "max_A", "min_B", "max_B", "change_direction"
        ).build();

        try (BufferedWriter writer = Files.newBufferedWriter(path,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
             CSVPrinter printer = new CSVPrinter(writer, format)) {

            for (MatchedConcept m : result.typeChanged) {
                String typeA = m.metaA.isCategorical() ? "CATEGORICAL" : "NUMERIC";
                String typeB = m.metaB.isCategorical() ? "CATEGORICAL" : "NUMERIC";
                String direction = m.metaA.isCategorical() ? "CAT_TO_NUM" : "NUM_TO_CAT";

                Integer catCountA = m.metaA.getCategoryValues() != null ? m.metaA.getCategoryValues().size() : null;
                Integer catCountB = m.metaB.getCategoryValues() != null ? m.metaB.getCategoryValues().size() : null;

                printer.printRecord(
                        m.conceptPath,
                        typeA,
                        typeB,
                        m.metaA.getObservationCount(),
                        m.metaB.getObservationCount(),
                        m.metaA.getPatientCount(),
                        m.metaB.getPatientCount(),
                        catCountA,
                        catCountB,
                        m.metaA.getMin(),
                        m.metaA.getMax(),
                        m.metaB.getMin(),
                        m.metaB.getMax(),
                        direction
                );
            }
        }

        log.info("✓ Wrote matching_type_changed.csv ({} rows)", result.typeChanged.size());
    }

    /**
     * Write matching_counts_changed.csv report.
     */
    private void writeMatchingCountsChanged(ComparisonResult result, Path path) throws IOException {
        CSVFormat format = CSVFormat.RFC4180.builder().setHeader(
                "conceptPath", "categorical", "observationCount_A", "observationCount_B",
                "observationCount_delta", "observationCount_pct_change",
                "patientCount_A", "patientCount_B", "patientCount_delta", "patientCount_pct_change"
        ).build();

        try (BufferedWriter writer = Files.newBufferedWriter(path,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
             CSVPrinter printer = new CSVPrinter(writer, format)) {

            for (MatchedConcept m : result.countsChanged) {
                long obsA = m.metaA.getObservationCount();
                long obsB = m.metaB.getObservationCount();
                long obsDelta = obsB - obsA;
                double obsPctChange = obsA > 0 ? (100.0 * obsDelta / obsA) : 0.0;

                long patA = m.metaA.getPatientCount();
                long patB = m.metaB.getPatientCount();
                long patDelta = patB - patA;
                double patPctChange = patA > 0 ? (100.0 * patDelta / patA) : 0.0;

                printer.printRecord(
                        m.conceptPath,
                        m.metaA.isCategorical(),
                        obsA,
                        obsB,
                        obsDelta,
                        String.format("%.2f%%", obsPctChange),
                        patA,
                        patB,
                        patDelta,
                        String.format("%.2f%%", patPctChange)
                );
            }
        }

        log.info("✓ Wrote matching_counts_changed.csv ({} rows)", result.countsChanged.size());
    }

    /**
     * Write matching_categories_changed.csv report.
     */
    private void writeMatchingCategoriesChanged(ComparisonResult result, Path path) throws IOException {
        CSVFormat format = CSVFormat.RFC4180.builder().setHeader(
                "conceptPath", "categoryCount_A", "categoryCount_B",
                "categoriesAdded", "categoriesRemoved",
                "categoriesAdded_list", "categoriesRemoved_list",
                "observationCount_A", "observationCount_B",
                "patientCount_A", "patientCount_B"
        ).build();

        try (BufferedWriter writer = Files.newBufferedWriter(path,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
             CSVPrinter printer = new CSVPrinter(writer, format)) {

            for (MatchedConcept m : result.categoriesChanged) {
                List<String> catsA = m.metaA.getCategoryValues() != null ? m.metaA.getCategoryValues() : Collections.emptyList();
                List<String> catsB = m.metaB.getCategoryValues() != null ? m.metaB.getCategoryValues() : Collections.emptyList();

                Set<String> setA = new HashSet<>(catsA);
                Set<String> setB = new HashSet<>(catsB);

                Set<String> added = new TreeSet<>(setB);
                added.removeAll(setA);

                Set<String> removed = new TreeSet<>(setA);
                removed.removeAll(setB);

                String addedList = String.join("µ", added);
                String removedList = String.join("µ", removed);

                printer.printRecord(
                        m.conceptPath,
                        catsA.size(),
                        catsB.size(),
                        added.size(),
                        removed.size(),
                        addedList,
                        removedList,
                        m.metaA.getObservationCount(),
                        m.metaB.getObservationCount(),
                        m.metaA.getPatientCount(),
                        m.metaB.getPatientCount()
                );
            }
        }

        log.info("✓ Wrote matching_categories_changed.csv ({} rows)", result.categoriesChanged.size());
    }

    /**
     * Write matching_minmax_changed.csv report.
     */
    private void writeMatchingMinMaxChanged(ComparisonResult result, Path path) throws IOException {
        CSVFormat format = CSVFormat.RFC4180.builder().setHeader(
                "conceptPath", "min_A", "min_B", "min_delta",
                "max_A", "max_B", "max_delta",
                "observationCount_A", "observationCount_B",
                "patientCount_A", "patientCount_B"
        ).build();

        try (BufferedWriter writer = Files.newBufferedWriter(path,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
             CSVPrinter printer = new CSVPrinter(writer, format)) {

            for (MatchedConcept m : result.minMaxChanged) {
                Double minA = m.metaA.getMin();
                Double minB = m.metaB.getMin();
                Double minDelta = (minA != null && minB != null) ? (minB - minA) : null;

                Double maxA = m.metaA.getMax();
                Double maxB = m.metaB.getMax();
                Double maxDelta = (maxA != null && maxB != null) ? (maxB - maxA) : null;

                printer.printRecord(
                        m.conceptPath,
                        minA,
                        minB,
                        minDelta,
                        maxA,
                        maxB,
                        maxDelta,
                        m.metaA.getObservationCount(),
                        m.metaB.getObservationCount(),
                        m.metaA.getPatientCount(),
                        m.metaB.getPatientCount()
                );
            }
        }

        log.info("✓ Wrote matching_minmax_changed.csv ({} rows)", result.minMaxChanged.size());
    }

    /**
     * Write missing_in_b.csv report.
     */
    private void writeMissingInB(ComparisonResult result, Path path) throws IOException {
        writeMissingReport(result.missingInB, result.metaA, path, "missing_in_b.csv");
    }

    /**
     * Write missing_in_a.csv report.
     */
    private void writeMissingInA(ComparisonResult result, Path path) throws IOException {
        writeMissingReport(result.missingInA, result.metaB, path, "missing_in_a.csv");
    }

    /**
     * Write missing concepts report (helper method).
     */
    private void writeMissingReport(Set<String> missingKeys, Map<String, ColumnMeta> metadata,
                                    Path path, String reportName) throws IOException {
        CSVFormat format = CSVFormat.RFC4180.builder().setHeader(
                "conceptPath", "categorical", "observationCount", "patientCount",
                "categoryCount", "min", "max"
        ).build();

        try (BufferedWriter writer = Files.newBufferedWriter(path,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
             CSVPrinter printer = new CSVPrinter(writer, format)) {

            for (String conceptPath : missingKeys) {
                ColumnMeta meta = metadata.get(conceptPath);

                Integer catCount = meta.getCategoryValues() != null ? meta.getCategoryValues().size() : null;

                printer.printRecord(
                        conceptPath,
                        meta.isCategorical(),
                        meta.getObservationCount(),
                        meta.getPatientCount(),
                        catCount,
                        meta.getMin(),
                        meta.getMax()
                );
            }
        }

        log.info("✓ Wrote {} ({} rows)", reportName, missingKeys.size());
    }

    /**
     * Get file size in bytes.
     */
    private long getSizeInBytes(String filePath) {
        try {
            return Files.size(Paths.get(filePath));
        } catch (IOException e) {
            return 0;
        }
    }

    // ============================================================
    // INNER CLASSES
    // ============================================================

    /**
     * Holds all comparison results.
     */
    static class ComparisonResult {
        String fileAPath;
        String fileBPath;
        Map<String, ColumnMeta> metaA;
        Map<String, ColumnMeta> metaB;
        long fileASize;
        long fileBSize;

        Set<String> matchingKeys = new TreeSet<>();
        Set<String> missingInB = new TreeSet<>();
        Set<String> missingInA = new TreeSet<>();

        List<MatchedConcept> identical = new ArrayList<>();
        List<MatchedConcept> typeChanged = new ArrayList<>();
        List<MatchedConcept> countsChanged = new ArrayList<>();
        List<MatchedConcept> categoriesChanged = new ArrayList<>();
        List<MatchedConcept> minMaxChanged = new ArrayList<>();
    }

    /**
     * Represents a matched concept with both metadata versions.
     */
    static class MatchedConcept {
        final String conceptPath;
        final ColumnMeta metaA;
        final ColumnMeta metaB;

        MatchedConcept(String conceptPath, ColumnMeta metaA, ColumnMeta metaB) {
            this.conceptPath = conceptPath;
            this.metaA = metaA;
            this.metaB = metaB;
        }

        boolean isTypeChanged() {
            return metaA.isCategorical() != metaB.isCategorical();
        }

        boolean isCountsChanged() {
            return metaA.getObservationCount() != metaB.getObservationCount()
                    || metaA.getPatientCount() != metaB.getPatientCount();
        }

        boolean isCategoriesChanged() {
            if (!metaA.isCategorical() || !metaB.isCategorical()) {
                return false; // Only check for categorical concepts
            }

            List<String> catsA = metaA.getCategoryValues() != null ? metaA.getCategoryValues() : Collections.emptyList();
            List<String> catsB = metaB.getCategoryValues() != null ? metaB.getCategoryValues() : Collections.emptyList();

            return !new HashSet<>(catsA).equals(new HashSet<>(catsB));
        }

        boolean isMinMaxChanged() {
            if (metaA.isCategorical() || metaB.isCategorical()) {
                return false; // Only check for numeric concepts
            }

            return !Objects.equals(metaA.getMin(), metaB.getMin())
                    || !Objects.equals(metaA.getMax(), metaB.getMax());
        }

        boolean isIdentical() {
            return !isTypeChanged() && !isCountsChanged() && !isCategoriesChanged() && !isMinMaxChanged();
        }
    }
}

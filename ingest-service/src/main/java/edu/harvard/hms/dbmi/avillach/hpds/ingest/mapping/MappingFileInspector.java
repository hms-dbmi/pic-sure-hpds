package edu.harvard.hms.dbmi.avillach.hpds.ingest.mapping;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Deterministic utility to inspect mapping files and print structure information.
 * Used to validate file format before parsing.
 */
public class MappingFileInspector {
    private static final Logger log = LoggerFactory.getLogger(MappingFileInspector.class);
    private static final int PREVIEW_LINES = 5;

    private static final Pattern HEADER_PATTERN = Pattern.compile(
        ".*(subject|patient|dbgap|consent|sample|mapping|identifier|id).*",
        Pattern.CASE_INSENSITIVE
    );

    public record InspectionResult(
        String absolutePath,
        long fileSizeBytes,
        List<String> rawLines,
        char detectedDelimiter,
        boolean hasHeader,
        int columnCount,
        String firstColumnName,
        String candidatePatientNumColumn
    ) {}

    /**
     * Inspect a mapping file and return structured information.
     */
    public static InspectionResult inspect(Path filePath) throws IOException {
        if (!Files.exists(filePath)) {
            throw new IOException("File not found: " + filePath);
        }
        if (!Files.isRegularFile(filePath)) {
            throw new IOException("Not a regular file: " + filePath);
        }

        long fileSize = Files.size(filePath);
        List<String> allLines = Files.readAllLines(filePath);

        if (allLines.isEmpty()) {
            throw new IOException("File is empty: " + filePath);
        }

        // Get first N lines for preview
        List<String> rawLines = allLines.stream()
            .limit(PREVIEW_LINES)
            .toList();

        // Detect delimiter deterministically from first line
        String firstLine = allLines.get(0);
        char delimiter = detectDelimiter(firstLine);

        // Check if first line is a header
        boolean hasHeader = HEADER_PATTERN.matcher(firstLine).matches();

        // Split first line to get column count
        String[] columns = splitLine(firstLine, delimiter);
        int columnCount = columns.length;

        // Get first column name (or value if no header)
        String firstColumnName = columns.length > 0 ? columns[0].replace("\"", "").trim() : "";

        // For CSV with header, try to find patient num column
        String candidatePatientNumColumn = null;
        if (hasHeader && delimiter == ',') {
            candidatePatientNumColumn = findPatientNumColumn(columns);
        }

        return new InspectionResult(
            filePath.toAbsolutePath().toString(),
            fileSize,
            rawLines,
            delimiter,
            hasHeader,
            columnCount,
            firstColumnName,
            candidatePatientNumColumn
        );
    }

    /**
     * Detect delimiter using deterministic heuristic: count occurrences, pick max.
     * Tie-breaking order: tab, comma, pipe.
     */
    private static char detectDelimiter(String line) {
        int tabCount = countOccurrences(line, '\t');
        int commaCount = countOccurrences(line, ',');
        int pipeCount = countOccurrences(line, '|');

        // Find max, with deterministic tie-breaking
        int max = Math.max(tabCount, Math.max(commaCount, pipeCount));

        if (max == 0) {
            // No delimiters found, default to tab
            return '\t';
        }

        // Tie-breaking order: tab, comma, pipe
        if (tabCount == max) return '\t';
        if (commaCount == max) return ',';
        return '|';
    }

    /**
     * Count occurrences of a character in a string.
     */
    private static int countOccurrences(String str, char ch) {
        return (int) str.chars().filter(c -> c == ch).count();
    }

    /**
     * Split line by delimiter, handling quoted CSV fields.
     */
    private static String[] splitLine(String line, char delimiter) {
        if (delimiter == ',') {
            // Simple CSV parsing (handles quoted fields)
            return line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
        } else {
            return line.split(String.valueOf(delimiter), -1);
        }
    }

    /**
     * Find candidate patient num column by name pattern.
     * Deterministic order: patient_num, patientNum, patient_number, hpds_patient_num, hpdsPatientNum.
     */
    private static String findPatientNumColumn(String[] columns) {
        String[] candidates = {
            "patient_num", "patientNum", "patient_number",
            "hpds_patient_num", "hpdsPatientNum"
        };

        for (String candidate : candidates) {
            for (int i = 0; i < columns.length; i++) {
                String col = columns[i].replace("\"", "").trim();
                if (col.equalsIgnoreCase(candidate)) {
                    return col + " (index=" + i + ")";
                }
            }
        }

        return null;
    }

    /**
     * Print inspection result to log.
     */
    public static void printInspection(InspectionResult result) {
        log.info("=== Mapping File Inspection ===");
        log.info("Path: {}", result.absolutePath);
        log.info("Size: {} bytes ({} KB)", result.fileSizeBytes, result.fileSizeBytes / 1024);
        log.info("Detected delimiter: '{}' ({})",
            getDelimiterName(result.detectedDelimiter),
            getDelimiterEscape(result.detectedDelimiter));
        log.info("Has header: {}", result.hasHeader);
        log.info("Column count: {}", result.columnCount);
        log.info("First column: {}", result.firstColumnName);
        if (result.candidatePatientNumColumn != null) {
            log.info("Candidate patientNum column: {}", result.candidatePatientNumColumn);
        }
        log.info("First {} lines:", PREVIEW_LINES);
        for (int i = 0; i < result.rawLines.size(); i++) {
            log.info("  [{}] {}", i + 1, result.rawLines.get(i));
        }
        log.info("=== End Inspection ===");
    }

    private static String getDelimiterName(char delimiter) {
        return switch (delimiter) {
            case '\t' -> "TAB";
            case ',' -> "COMMA";
            case '|' -> "PIPE";
            default -> "UNKNOWN";
        };
    }

    private static String getDelimiterEscape(char delimiter) {
        return switch (delimiter) {
            case '\t' -> "\\t";
            case ',' -> ",";
            case '|' -> "|";
            default -> String.valueOf(delimiter);
        };
    }
}

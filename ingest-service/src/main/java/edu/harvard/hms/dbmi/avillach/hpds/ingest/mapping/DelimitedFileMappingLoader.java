package edu.harvard.hms.dbmi.avillach.hpds.ingest.mapping;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Generic loader for delimited mapping files.
 * Supports TSV/CSV with or without headers.
 */
public class DelimitedFileMappingLoader {
    private static final Logger log = LoggerFactory.getLogger(DelimitedFileMappingLoader.class);

    /**
     * Configuration for loading a mapping file.
     */
    public record LoaderConfig(
        Path filePath,
        char delimiter,
        boolean hasHeader,
        String keyColumnName,      // used if hasHeader=true
        String valueColumnName,    // used if hasHeader=true
        int keyColumnIndex,        // used if hasHeader=false (0-based)
        int valueColumnIndex       // used if hasHeader=false (0-based)
    ) {
        public static LoaderConfig withHeader(Path filePath, char delimiter, String keyColumn, String valueColumn) {
            return new LoaderConfig(filePath, delimiter, true, keyColumn, valueColumn, -1, -1);
        }

        public static LoaderConfig withoutHeader(Path filePath, char delimiter, int keyIndex, int valueIndex) {
            return new LoaderConfig(filePath, delimiter, false, null, null, keyIndex, valueIndex);
        }
    }

    /**
     * Load a mapping file into a map: key -> value.
     */
    public static Map<String, String> loadMapping(LoaderConfig config) throws IOException {
        if (!Files.exists(config.filePath)) {
            throw new IOException("Mapping file not found: " + config.filePath);
        }

        Map<String, String> mapping = new HashMap<>();

        try (BufferedReader reader = Files.newBufferedReader(config.filePath)) {
            int keyIdx = config.keyColumnIndex;
            int valueIdx = config.valueColumnIndex;

            // Parse header if present
            if (config.hasHeader) {
                String headerLine = reader.readLine();
                if (headerLine == null) {
                    throw new IOException("Mapping file is empty: " + config.filePath);
                }

                String[] headers = splitLine(headerLine, config.delimiter);
                keyIdx = findColumnIndex(headers, config.keyColumnName);
                valueIdx = findColumnIndex(headers, config.valueColumnName);

                if (keyIdx == -1) {
                    throw new IOException(String.format(
                        "Key column '%s' not found in %s. Available columns: %s",
                        config.keyColumnName, config.filePath, String.join(", ", headers)
                    ));
                }
                if (valueIdx == -1) {
                    throw new IOException(String.format(
                        "Value column '%s' not found in %s. Available columns: %s",
                        config.valueColumnName, config.filePath, String.join(", ", headers)
                    ));
                }

                log.debug("Found columns: {} at index {}, {} at index {}",
                    config.keyColumnName, keyIdx, config.valueColumnName, valueIdx);
            }

            // Parse data lines
            String line;
            int lineNum = config.hasHeader ? 1 : 0;
            while ((line = reader.readLine()) != null) {
                lineNum++;
                if (line.isBlank()) {
                    continue;
                }

                String[] columns = splitLine(line, config.delimiter);
                if (columns.length <= Math.max(keyIdx, valueIdx)) {
                    log.warn("Skipping malformed line {} in {}: insufficient columns (expected at least {})",
                        lineNum, config.filePath.getFileName(), Math.max(keyIdx, valueIdx) + 1);
                    continue;
                }

                String key = columns[keyIdx].trim();
                String value = columns[valueIdx].trim();

                if (key.isEmpty() || value.isEmpty()) {
                    log.warn("Skipping line {} in {}: empty key or value", lineNum, config.filePath.getFileName());
                    continue;
                }

                mapping.put(key, value);
            }
        } catch (IOException e) {
            throw new IOException("Failed to load mapping from " + config.filePath + ": " + e.getMessage(), e);
        }

        if (mapping.isEmpty()) {
            throw new IOException("No valid mappings found in: " + config.filePath);
        }

        log.info("Loaded {} mappings from {}", mapping.size(), config.filePath);
        return mapping;
    }

    /**
     * Split line by delimiter, handling quoted CSV fields.
     */
    private static String[] splitLine(String line, char delimiter) {
        if (delimiter == ',') {
            // CSV with quote handling
            String[] fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
            for (int i = 0; i < fields.length; i++) {
                fields[i] = fields[i].replace("\"", "");
            }
            return fields;
        } else {
            return line.split(String.valueOf(delimiter), -1);
        }
    }

    /**
     * Find column index by name (case-insensitive).
     */
    private static int findColumnIndex(String[] headers, String columnName) {
        for (int i = 0; i < headers.length; i++) {
            if (headers[i].trim().equalsIgnoreCase(columnName)) {
                return i;
            }
        }
        return -1;
    }
}

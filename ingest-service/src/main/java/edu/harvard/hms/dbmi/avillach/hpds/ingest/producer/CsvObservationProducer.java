package edu.harvard.hms.dbmi.avillach.hpds.ingest.producer;

import edu.harvard.hms.dbmi.avillach.hpds.ingest.failure.FailureReason;
import edu.harvard.hms.dbmi.avillach.hpds.ingest.failure.FailureRecord;
import edu.harvard.hms.dbmi.avillach.hpds.ingest.failure.FailureSink;
import edu.harvard.hms.dbmi.avillach.hpds.writer.ObservationRow;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Produces observations from legacy split CSV files.
 *
 * CSV format (allConcepts schema):
 * Supports both header-based and positional parsing:
 *
 * With headers (case-insensitive):
 *   PATIENT_NUM, CONCEPT_PATH, NVAL_NUM, TVAL_CHAR, TIMESTAMP
 *
 * Without headers (positional):
 * - Column 0: PATIENT_NUM (integer)
 * - Column 1: CONCEPT_PATH (backslash-delimited string, already formatted)
 * - Column 2: NVAL_NUM (numeric value, nullable, empty string if null)
 * - Column 3: TVAL_CHAR (text value, nullable, empty string if null)
 * - Column 4: TIMESTAMP (ISO 8601 or empty, nullable)
 */
public class CsvObservationProducer {
    private static final Logger log = LoggerFactory.getLogger(CsvObservationProducer.class);

    private final String runId;
    private final FailureSink failureSink;

    // Expected header names (case-insensitive)
    private static final String HEADER_PATIENT_NUM = "PATIENT_NUM";
    private static final String HEADER_CONCEPT_PATH = "CONCEPT_PATH";
    private static final String HEADER_NVAL_NUM = "NVAL_NUM";
    private static final String HEADER_TVAL_CHAR = "TVAL_CHAR";
    private static final String HEADER_TIMESTAMP = "TIMESTAMP";

    public CsvObservationProducer(String runId, FailureSink failureSink) {
        this.runId = runId;
        this.failureSink = failureSink;
    }

    /**
     * Processes a single CSV file with streaming parser.
     * Automatically detects if file has headers or is positional.
     *
     * @param filePath path to CSV file
     * @param consumer callback for each batch
     * @param batchSize rows per batch
     */
    public void processFile(Path filePath, Consumer<List<ObservationRow>> consumer, int batchSize) throws IOException {
        log.info("Processing CSV file: {}", filePath);

        try (BufferedReader reader = Files.newBufferedReader(filePath)) {
            // Read first line to detect headers
            String firstLine = reader.readLine();
            if (firstLine == null || firstLine.isBlank()) {
                log.warn("CSV file is empty: {}", filePath);
                return;
            }

            // Parse first line to detect if it's a header
            boolean useHeaders = detectHeadersFromLine(firstLine);

            // Configure parser based on detection
            CSVFormat format;
            if (useHeaders) {
                log.info("Detected header row in {}, using named column access", filePath.getFileName());
                // Parse with headers, skip first line (it's the header)
                format = CSVFormat.DEFAULT
                    .builder()
                    .setHeader(HEADER_PATIENT_NUM, HEADER_CONCEPT_PATH, HEADER_NVAL_NUM, HEADER_TVAL_CHAR, HEADER_TIMESTAMP)
                    .setSkipHeaderRecord(false) // We already read it
                    .setIgnoreEmptyLines(true)
                    .setTrim(true)
                    .setQuote('"')
                    .build();
            } else {
                log.info("No valid header detected in {}, using positional column access", filePath.getFileName());
                // Parse without headers, need to re-process first line as data
                format = CSVFormat.DEFAULT
                    .builder()
                    .setIgnoreEmptyLines(true)
                    .setTrim(true)
                    .setQuote('"')
                    .build();

                // Reset reader to beginning to reprocess first line as data
                reader.close();
                BufferedReader newReader = Files.newBufferedReader(filePath);
                processWithFormat(newReader, filePath, format, useHeaders, consumer, batchSize);
                return;
            }

            // For header mode, continue with current reader position (after first line)
            processWithFormat(reader, filePath, format, useHeaders, consumer, batchSize);
        }
    }

    /**
     * Process CSV with the configured format.
     */
    private void processWithFormat(BufferedReader reader, Path filePath, CSVFormat format, boolean useHeaders,
                                   Consumer<List<ObservationRow>> consumer, int batchSize) throws IOException {
        try (CSVParser parser = new CSVParser(reader, format)) {
            List<ObservationRow> batch = new ArrayList<>(batchSize);

            for (CSVRecord record : parser) {
                try {
                    ObservationRow row = parseRecord(record, filePath, useHeaders);
                    if (row != null) {
                        batch.add(row);

                        if (batch.size() >= batchSize) {
                            consumer.accept(new ArrayList<>(batch));
                            batch.clear();
                        }
                    }
                } catch (Exception e) {
                    log.warn("Failed to parse CSV record at line {}: {}", record.getRecordNumber(), e.getMessage());
                    recordFailure(filePath.toString(), null, FailureReason.UNKNOWN, e.getMessage());
                }
            }

            // Flush remaining
            if (!batch.isEmpty()) {
                consumer.accept(batch);
            }

            log.info("Completed processing CSV file: {}", filePath);
        }
    }

    /**
     * Detects if a CSV line is a valid header row by parsing it first.
     */
    private boolean detectHeadersFromLine(String line) {
        try {
            // Parse the line
            CSVParser parser = CSVParser.parse(line, CSVFormat.DEFAULT.builder().setQuote('"').build());
            CSVRecord record = parser.iterator().next();
            parser.close();

            return detectHeaders(record);
        } catch (Exception e) {
            log.debug("Failed to parse first line as CSV, assuming positional: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Detects if the first record is a valid header row.
     * Returns true if all expected column names are present (case-insensitive).
     */
    private boolean detectHeaders(CSVRecord record) {
        if (record.size() < 4) {
            return false; // Too few columns to be a valid header
        }

        // Check if first row contains expected header names (case-insensitive)
        try {
            String col0 = record.get(0).trim().toUpperCase();
            String col1 = record.get(1).trim().toUpperCase();
            String col2 = record.get(2).trim().toUpperCase();
            String col3 = record.get(3).trim().toUpperCase();

            // Match expected headers
            boolean hasPatientNum = col0.equals(HEADER_PATIENT_NUM);
            boolean hasConceptPath = col1.equals(HEADER_CONCEPT_PATH);
            boolean hasNvalNum = col2.equals(HEADER_NVAL_NUM);
            boolean hasTvalChar = col3.equals(HEADER_TVAL_CHAR);

            return hasPatientNum && hasConceptPath && hasNvalNum && hasTvalChar;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Parses a CSV record into an ObservationRow.
     *
     * @param record CSV record
     * @param filePath source file path
     * @param useHeaders if true, use named column access; if false, use positional
     */
    private ObservationRow parseRecord(CSVRecord record, Path filePath, boolean useHeaders) {
        // Validate minimum columns (positional mode only)
        if (!useHeaders && record.size() < 4) {
            recordFailure(filePath.toString(), null, FailureReason.UNKNOWN,
                "Record has only " + record.size() + " columns, expected at least 4");
            return null;
        }

        // Extract PATIENT_NUM
        String patientNumRaw;
        try {
            patientNumRaw = useHeaders ? record.get(HEADER_PATIENT_NUM) : record.get(0);
        } catch (IllegalArgumentException e) {
            recordFailure(filePath.toString(), null, FailureReason.MISSING_PATIENT_ID,
                "PATIENT_NUM column not found: " + e.getMessage());
            return null;
        }

        if (patientNumRaw == null || patientNumRaw.isBlank()) {
            recordFailure(filePath.toString(), null, FailureReason.MISSING_PATIENT_ID, "PATIENT_NUM is null or empty");
            return null;
        }

        int patientNum;
        try {
            patientNum = Integer.parseInt(patientNumRaw.trim());
        } catch (NumberFormatException e) {
            recordFailure(filePath.toString(), patientNumRaw, FailureReason.INVALID_PATIENT_ID, "Cannot parse as integer: " + patientNumRaw);
            return null;
        }

        // Extract CONCEPT_PATH
        String conceptPath;
        try {
            conceptPath = useHeaders ? record.get(HEADER_CONCEPT_PATH) : record.get(1);
        } catch (IllegalArgumentException e) {
            recordFailure(filePath.toString(), String.valueOf(patientNum), FailureReason.MISSING_CONCEPT_PATH,
                "CONCEPT_PATH column not found: " + e.getMessage());
            return null;
        }

        if (conceptPath == null || conceptPath.isBlank()) {
            recordFailure(filePath.toString(), String.valueOf(patientNum), FailureReason.MISSING_CONCEPT_PATH, "CONCEPT_PATH is null or empty");
            return null;
        }

        // Ensure trailing delimiter
        if (!conceptPath.endsWith("\\")) {
            conceptPath = conceptPath + "\\";
        }

        // Extract NVAL_NUM and TVAL_CHAR
        String nvalNumRaw;
        String tvalChar;
        try {
            nvalNumRaw = useHeaders ? record.get(HEADER_NVAL_NUM) : record.get(2);
            tvalChar = useHeaders ? record.get(HEADER_TVAL_CHAR) : record.get(3);
        } catch (IllegalArgumentException e) {
            recordFailure(filePath.toString(), String.valueOf(patientNum), FailureReason.UNKNOWN,
                "Value column not found: " + e.getMessage());
            return null;
        }

        Double numericValue = null;
        String textValue = null;

        if (nvalNumRaw != null && !nvalNumRaw.isBlank()) {
            try {
                numericValue = Double.parseDouble(nvalNumRaw);
            } catch (NumberFormatException e) {
                recordFailure(filePath.toString(), String.valueOf(patientNum), FailureReason.NUMERIC_PARSE_ERROR,
                    "Cannot parse NVAL_NUM: " + nvalNumRaw);
                return null;
            }
        }

        if (tvalChar != null && !tvalChar.isBlank()) {
            textValue = tvalChar;
        }

        // Validate at least one value (allow both for flexibility)
        if (numericValue == null && textValue == null) {
            recordFailure(filePath.toString(), String.valueOf(patientNum), FailureReason.MISSING_VALUE,
                "Both NVAL_NUM and TVAL_CHAR are null");
            return null;
        }

        // Extract timestamp (optional)
        Instant timestamp = null;
        try {
            String timestampRaw;
            if (useHeaders) {
                // Try to get TIMESTAMP header, but don't fail if missing
                try {
                    timestampRaw = record.get(HEADER_TIMESTAMP);
                } catch (IllegalArgumentException e) {
                    timestampRaw = null; // Column not present
                }
            } else {
                // Positional: column 4 if present
                timestampRaw = record.size() > 4 ? record.get(4) : null;
            }

            if (timestampRaw != null && !timestampRaw.isBlank()) {
                try {
                    timestamp = Instant.parse(timestampRaw);
                } catch (DateTimeParseException e) {
                    // Timestamp parsing failures are non-fatal, log and continue
                    log.debug("Cannot parse timestamp '{}' for patient {}, continuing without timestamp", timestampRaw, patientNum);
                }
            }
        } catch (Exception e) {
            // Timestamp extraction is optional, continue without it
            log.debug("Timestamp extraction failed for patient {}: {}", patientNum, e.getMessage());
        }

        return new ObservationRow(patientNum, conceptPath, numericValue, textValue, timestamp);
    }

    /**
     * Records a failure to the sink.
     */
    private void recordFailure(String inputFile, String participantId, FailureReason reason, String detail) {
        FailureRecord record = new FailureRecord(
            runId,
            FailureRecord.SourceType.CSV,
            "legacy-csv",
            inputFile,
            participantId,
            null,  // dbgapSubjectId (not applicable for CSV)
            null,
            null,
            null,
            null,
            null,
            null,
            reason,
            detail
        );
        failureSink.recordFailure(record);
    }
}

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
 * - PATIENT_NUM
 * - CONCEPT_PATH (already formatted with backslash delimiters)
 * - NVAL_NUM (numeric value, nullable)
 * - TVAL_CHAR (text value, nullable)
 * - timestamp (ISO 8601)
 */
public class CsvObservationProducer {
    private static final Logger log = LoggerFactory.getLogger(CsvObservationProducer.class);

    private final String runId;
    private final FailureSink failureSink;

    public CsvObservationProducer(String runId, FailureSink failureSink) {
        this.runId = runId;
        this.failureSink = failureSink;
    }

    /**
     * Processes a single CSV file with streaming parser.
     *
     * @param filePath path to CSV file
     * @param consumer callback for each batch
     * @param batchSize rows per batch
     */
    public void processFile(Path filePath, Consumer<List<ObservationRow>> consumer, int batchSize) throws IOException {
        log.info("Processing CSV file: {}", filePath);

        try (BufferedReader reader = Files.newBufferedReader(filePath);
             CSVParser parser = new CSVParser(reader, CSVFormat.DEFAULT
                 .builder()
                 .setHeader()
                 .setSkipHeaderRecord(true)
                 .setIgnoreEmptyLines(true)
                 .setTrim(true)
                 .build())) {

            List<ObservationRow> batch = new ArrayList<>(batchSize);

            for (CSVRecord record : parser) {
                try {
                    ObservationRow row = parseRecord(record, filePath);
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
     * Parses a CSV record into an ObservationRow.
     */
    private ObservationRow parseRecord(CSVRecord record, Path filePath) {
        // Extract PATIENT_NUM
        String patientNumRaw = record.get("PATIENT_NUM");
        if (patientNumRaw == null || patientNumRaw.isBlank()) {
            recordFailure(filePath.toString(), null, FailureReason.MISSING_PATIENT_ID, "PATIENT_NUM is null or empty");
            return null;
        }

        int patientNum;
        try {
            patientNum = Integer.parseInt(patientNumRaw.trim());
        } catch (NumberFormatException e) {
            recordFailure(filePath.toString(), patientNumRaw, FailureReason.INVALID_PATIENT_ID, "Cannot parse as integer");
            return null;
        }

        // Extract CONCEPT_PATH
        String conceptPath = record.get("CONCEPT_PATH");
        if (conceptPath == null || conceptPath.isBlank()) {
            recordFailure(filePath.toString(), patientNumRaw, FailureReason.MISSING_CONCEPT_PATH, "CONCEPT_PATH is null or empty");
            return null;
        }

        // Ensure trailing delimiter
        if (!conceptPath.endsWith("\\")) {
            conceptPath = conceptPath + "\\";
        }

        // Extract values
        String nvalNumRaw = record.get("NVAL_NUM");
        String tvalChar = record.get("TVAL_CHAR");

        Double numericValue = null;
        String textValue = null;

        if (nvalNumRaw != null && !nvalNumRaw.isBlank()) {
            try {
                numericValue = Double.parseDouble(nvalNumRaw);
            } catch (NumberFormatException e) {
                recordFailure(filePath.toString(), patientNumRaw, FailureReason.NUMERIC_PARSE_ERROR,
                    "Cannot parse NVAL_NUM: " + nvalNumRaw);
                return null;
            }
        }

        if (tvalChar != null && !tvalChar.isBlank()) {
            textValue = tvalChar;
        }

        // Validate exactly one value
        if (numericValue == null && textValue == null) {
            recordFailure(filePath.toString(), patientNumRaw, FailureReason.MISSING_VALUE,
                "Both NVAL_NUM and TVAL_CHAR are null");
            return null;
        }

        if (numericValue != null && textValue != null) {
            recordFailure(filePath.toString(), patientNumRaw, FailureReason.DUPLICATE_VALUE,
                "Both NVAL_NUM and TVAL_CHAR are non-null");
            return null;
        }

        // Extract timestamp
        Instant timestamp = null;
        String timestampRaw = record.get("timestamp");
        if (timestampRaw != null && !timestampRaw.isBlank()) {
            try {
                timestamp = Instant.parse(timestampRaw);
            } catch (DateTimeParseException e) {
                recordFailure(filePath.toString(), patientNumRaw, FailureReason.INVALID_TIMESTAMP,
                    "Cannot parse timestamp: " + timestampRaw);
                // Continue without timestamp
            }
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

package edu.harvard.hms.dbmi.avillach.hpds.ingest.producer;

import edu.harvard.hms.dbmi.avillach.hpds.ingest.config.ParquetDatasetConfig;
import edu.harvard.hms.dbmi.avillach.hpds.ingest.failure.FailureReason;
import edu.harvard.hms.dbmi.avillach.hpds.ingest.failure.FailureRecord;
import edu.harvard.hms.dbmi.avillach.hpds.ingest.failure.FailureSink;
import edu.harvard.hms.dbmi.avillach.hpds.ingest.mapping.PatientIdResolver;
import edu.harvard.hms.dbmi.avillach.hpds.writer.ObservationRow;
import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Produces observations from Parquet files using Apache Arrow Dataset API.
 *
 * - Schema projection (read only needed columns)
 * - Bounded memory (record batch streaming)
 */
public class ParquetObservationProducer {
    private static final Logger log = LoggerFactory.getLogger(ParquetObservationProducer.class);

    // HPDS uses backslash delimiter
    private static final String DELIMITER = "\\";

    private final String runId;
    private final ParquetDatasetConfig config;
    private final FailureSink failureSink;
    private final PatientIdResolver patientIdResolver;
    private final BufferAllocator allocator;

    public ParquetObservationProducer(String runId, ParquetDatasetConfig config, FailureSink failureSink, PatientIdResolver patientIdResolver) {
        this.runId = runId;
        this.config = config;
        this.failureSink = failureSink;
        this.patientIdResolver = patientIdResolver;
        this.allocator = new RootAllocator(Long.MAX_VALUE);
    }

    /**
     * Processes a single Parquet file with bounded memory using Arrow Dataset API.
     *
     * @param filePath path to Parquet file
     * @param consumer callback for each batch of observations
     * @param batchSize number of rows per batch
     */
    public void processFile(java.nio.file.Path filePath, Consumer<List<ObservationRow>> consumer, int batchSize) throws IOException {
        log.debug("Processing Parquet file: {}", filePath);

        String fileUri = filePath.toUri().toString();

        // Build column projection (only read needed columns)
        List<String> columns = new ArrayList<>();
        columns.add(config.participantIdColumn());
        if (!"none".equalsIgnoreCase(config.timestampColumn())) {
            columns.add(config.timestampColumn());
        }
        columns.addAll(config.variableColumns());

        ScanOptions options = new ScanOptions.Builder(batchSize)
            .columns(Optional.of(columns.toArray(new String[0])))
            .build();

        try (DatasetFactory datasetFactory = new FileSystemDatasetFactory(
                allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, fileUri);
             Dataset dataset = datasetFactory.finish();
             Scanner scanner = dataset.newScan(options);
             ArrowReader reader = scanner.scanBatches()) {

            List<ObservationRow> batch = new ArrayList<>(batchSize);
            long rowCount = 0;

            while (reader.loadNextBatch()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                int rows = root.getRowCount();

                for (int i = 0; i < rows; i++) {
                    rowCount++;
                    try {
                        List<ObservationRow> rowObservations = parseRow(root, i, filePath);
                        batch.addAll(rowObservations);

                        if (batch.size() >= batchSize) {
                            consumer.accept(new ArrayList<>(batch));
                            batch.clear();
                        }
                    } catch (Exception e) {
                        log.warn("Failed to parse row {} in file {}: {}", rowCount, filePath, e.getMessage());
                        recordFailure(filePath.toString(), null, null, FailureReason.UNKNOWN, e.getMessage());
                    }
                }
            }

            // Flush remaining batch
            if (!batch.isEmpty()) {
                consumer.accept(batch);
            }

            log.debug("Completed processing file: {} ({} rows)", filePath, rowCount);
        } catch (Exception e) {
            throw new IOException("Failed to process Parquet file: " + filePath, e);
        }
    }

    /**
     * Parses a single row from Arrow VectorSchemaRoot into ObservationRows (one per variable).
     */
    private List<ObservationRow> parseRow(VectorSchemaRoot root, int rowIndex, java.nio.file.Path filePath) {
        List<ObservationRow> rows = new ArrayList<>();

        // Extract participant ID
        FieldVector participantVector = root.getVector(config.participantIdColumn());
        if (participantVector == null || participantVector.isNull(rowIndex)) {
            recordFailure(filePath.toString(), null, null, FailureReason.MISSING_PATIENT_ID, "Participant ID is null or empty");
            return rows;
        }

        String participantIdRaw = participantVector.getObject(rowIndex).toString();

        // Resolve participant ID to HPDS patient number
        PatientIdResolver.ResolutionResult resolution = patientIdResolver.resolvePatientNum(participantIdRaw);
        if (!resolution.success()) {
            // Map reason code string to enum, with fallback to UNKNOWN
            FailureReason reason;
            try {
                reason = FailureReason.valueOf(resolution.failureReasonCode());
            } catch (IllegalArgumentException e) {
                log.warn("Unknown failure reason code '{}', using UNKNOWN", resolution.failureReasonCode());
                reason = FailureReason.UNKNOWN;
            }

            recordFailure(
                filePath.toString(),
                participantIdRaw,
                resolution.dbgapSubjectId(),
                reason,
                resolution.failureReasonDetail()
            );
            return rows;
        }

        int patientNum = resolution.patientNum();

        // Extract timestamp
        Instant timestamp = parseTimestamp(root, rowIndex, filePath, participantIdRaw);

        // Process each configured variable
        for (ParquetDatasetConfig.VariableConfig varConfig : config.variables()) {
            FieldVector varVector = root.getVector(varConfig.column());
            if (varVector == null || varVector.isNull(rowIndex)) {
                continue; // Skip null values (not a failure)
            }

            String valueRaw = varVector.getObject(rowIndex).toString();
            if (valueRaw.isBlank()) {
                continue;
            }

            // Build concept path from config: \prefix[0]\prefix[1]\...\<device>\<variable>\
            StringBuilder conceptPath = new StringBuilder();
            if (config.conceptPathPrefix() != null && !config.conceptPathPrefix().isEmpty()) {
                for (String prefix : config.conceptPathPrefix()) {
                    conceptPath.append(DELIMITER).append(prefix);
                }
            }
            conceptPath.append(DELIMITER).append(config.deviceName());
            conceptPath.append(DELIMITER).append(varConfig.label());
            conceptPath.append(DELIMITER);

            // Determine if numeric or categorical based on forceType or auto-detect
            Double numericValue = null;
            String textValue = null;

            if ("NUMERIC".equalsIgnoreCase(varConfig.forceType())) {
                try {
                    numericValue = Double.parseDouble(valueRaw);
                } catch (NumberFormatException e) {
                    recordFailure(filePath.toString(), participantIdRaw, resolution.dbgapSubjectId(), FailureReason.NUMERIC_PARSE_ERROR,
                        "Cannot parse as numeric: " + valueRaw);
                    continue;
                }
            } else if ("TEXT".equalsIgnoreCase(varConfig.forceType())) {
                textValue = valueRaw;
            } else {
                // Auto-detect
                numericValue = tryParseNumeric(valueRaw);
                textValue = (numericValue == null) ? valueRaw : null;
            }

            rows.add(new ObservationRow(patientNum, conceptPath.toString(), numericValue, textValue, timestamp));
        }

        return rows;
    }

    /**
     * Parses timestamp from configured column.
     * Returns null if timestamp cannot be parsed (timestamps are optional in HPDS).
     */
    private Instant parseTimestamp(VectorSchemaRoot root, int rowIndex, java.nio.file.Path filePath, String participantId) {
        if ("none".equalsIgnoreCase(config.timestampColumn())) {
            return null;
        }

        FieldVector timestampVector = root.getVector(config.timestampColumn());
        if (timestampVector == null || timestampVector.isNull(rowIndex)) {
            // Timestamp column missing or null - this is OK, timestamps are optional
            return null;
        }

        String timestampRaw = timestampVector.getObject(rowIndex).toString();

        // Treat common "null-like" values as null timestamp
        if (timestampRaw.isBlank() || "None".equalsIgnoreCase(timestampRaw) || "null".equalsIgnoreCase(timestampRaw)) {
            return null;
        }

        // Deterministic timestamp parsing with strict fallback chain:
        // 1. Try Instant.parse() - accepts ISO-8601 with zone (Z or offset like +00:00)
        // 2. Try LocalDateTime.parse() - accepts ISO-8601 local date-time, treat as UTC
        // 3. Try LocalDate.parse() - accepts yyyy-MM-dd, treat as midnight UTC
        // 4. Return null (timestamps are optional, don't fail the row)

        try {
            // Attempt 1: Parse as Instant (requires Z or offset)
            return Instant.parse(timestampRaw);
        } catch (DateTimeParseException e1) {
            try {
                // Attempt 2: Parse as LocalDateTime (e.g., 2025-02-28T12:56:40.500), treat as UTC
                java.time.LocalDateTime localDateTime = java.time.LocalDateTime.parse(timestampRaw, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                return localDateTime.toInstant(ZoneOffset.UTC);
            } catch (DateTimeParseException e2) {
                try {
                    // Attempt 3: Parse as LocalDate (e.g., 2025-02-28), treat as midnight UTC
                    LocalDate date = LocalDate.parse(timestampRaw, DateTimeFormatter.ISO_LOCAL_DATE);
                    return date.atStartOfDay().toInstant(ZoneOffset.UTC);
                } catch (DateTimeParseException e3) {
                    // Attempt 4: All parsing failed - log warning but continue (timestamps are optional)
                    log.warn("Cannot parse timestamp for participant {}, using null: {}", participantId, timestampRaw);
                    return null;
                }
            }
        }
    }

    /**
     * Attempts to parse a string as Double.
     */
    private Double tryParseNumeric(String value) {
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Records a failure to the sink.
     */
    private void recordFailure(String inputFile, String participantId, String dbgapSubjectId, FailureReason reason, String detail) {
        FailureRecord record = new FailureRecord(
            runId,
            FailureRecord.SourceType.PARQUET,
            config.datasetName(),
            inputFile,
            participantId,
            dbgapSubjectId,
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

    public void close() {
        allocator.close();
    }
}

package edu.harvard.hms.dbmi.avillach.hpds.ingest.failure;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * JSONL record for failure capture (Splunk/Elastic compatible).
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record FailureRecord(
    String runId,
    SourceType sourceType,
    String dataset,
    String inputFile,
    String participantId,
    String dbgapSubjectId,  // intermediate mapping value
    Integer patientNum,
    String conceptPath,
    String timestampRaw,
    String timestampParsed,
    String valueRaw,
    String numericParseError,
    FailureReason reasonCode,
    String reasonDetail
) {
    public enum SourceType {
        PARQUET,
        CSV
    }
}

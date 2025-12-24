package edu.harvard.hms.dbmi.avillach.hpds.ingest.mapping;

/**
 * Resolves Parquet participant identifiers to HPDS patient numbers.
 */
public interface PatientIdResolver {

    /**
     * Result of patient ID resolution.
     */
    record ResolutionResult(
        boolean success,
        Integer patientNum,           // null if resolution failed
        String dbgapSubjectId,        // intermediate value, may be null if first step failed
        String failureReasonCode,     // null if success
        String failureReasonDetail    // null if success
    ) {
        public static ResolutionResult success(int patientNum, String dbgapSubjectId) {
            return new ResolutionResult(true, patientNum, dbgapSubjectId, null, null);
        }

        public static ResolutionResult failure(String reasonCode, String reasonDetail) {
            return new ResolutionResult(false, null, null, reasonCode, reasonDetail);
        }

        public static ResolutionResult failure(String reasonCode, String reasonDetail, String dbgapSubjectId) {
            return new ResolutionResult(false, null, dbgapSubjectId, reasonCode, reasonDetail);
        }
    }

    /**
     * Resolve participant ID to HPDS patient number.
     *
     * @param participantId participant identifier from Parquet (e.g., "RA11")
     * @return resolution result with patient num or failure reason
     */
    ResolutionResult resolvePatientNum(String participantId);

    /**
     * Get statistics about loaded mappings.
     */
    MappingStatistics getStatistics();

    /**
     * Statistics about loaded mappings.
     */
    record MappingStatistics(
        int dbgapMappingCount,
        int patientMappingCount,
        String dbgapMappingFile,
        String patientMappingFile
    ) {}
}

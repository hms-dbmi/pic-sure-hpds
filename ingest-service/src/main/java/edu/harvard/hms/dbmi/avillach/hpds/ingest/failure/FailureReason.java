package edu.harvard.hms.dbmi.avillach.hpds.ingest.failure;

/**
 * Stable enumeration of failure reasons for Splunk/Elastic consumption.
 */
public enum FailureReason {
    MISSING_PATIENT_ID("Patient ID column missing or null"),
    INVALID_PATIENT_ID("Patient ID not an integer"),
    INVALID_PARTICIPANT_ID("Participant ID is null or empty"),
    INVALID_PATIENT_NUM_DIRECT("Participant ID parses as non-positive integer"),
    INVALID_PATIENT_NUM_THIRD_COLUMN("Third column patient number is non-positive"),
    MISSING_STEP1_MAPPING("No mapping found in step 1 of mapping chain"),
    MISSING_STEP2_MAPPING("No mapping found in step 2 of mapping chain"),
    MISSING_DBGAP_SUBJECT("No dbGaP subject ID found for participant"),
    MISSING_PATIENT_MAPPING("No HPDS patient number found for dbGaP subject ID"),
    MAPPING_PARSE_ERROR("Error parsing mapping file"),
    MAPPING_CONFIG_ERROR("Mapping file configuration error"),
    MISSING_CONCEPT_PATH("Concept path missing or empty"),
    INVALID_CONCEPT_PATH("Concept path malformed"),
    MISSING_TIMESTAMP_COLUMN("Configured timestamp column not found"),
    INVALID_TIMESTAMP("Timestamp unparseable"),
    MISSING_VALUE("Both numeric and text values are null"),
    DUPLICATE_VALUE("Both numeric and text values are non-null"),
    NUMERIC_PARSE_ERROR("Numeric value unparseable"),
    SCHEMA_MISMATCH("Parquet schema does not match config"),
    FILE_READ_ERROR("Unable to read source file"),
    DATASET_SKIPPED_NO_TIMESTAMP("Dataset skipped (no timestamp configured)"),
    UNKNOWN("Unknown failure");

    private final String description;

    FailureReason(String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }
}

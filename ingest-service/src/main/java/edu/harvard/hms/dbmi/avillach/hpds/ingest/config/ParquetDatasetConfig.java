package edu.harvard.hms.dbmi.avillach.hpds.ingest.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Configuration for a single Parquet dataset.
 * Typically loaded from JSONL config file.
 */
public record ParquetDatasetConfig(
    @JsonProperty("dataset") String datasetName,
    @JsonProperty("device") String deviceName,              // e.g., "FitBit", "Garmin"
    String participantIdColumn,
    String timestampColumn,         // or "none" for relational child tables
    List<VariableConfig> variables, // variables to ingest
    @JsonProperty("conceptPathPrefix") List<String> conceptPathPrefix  // e.g., ["phs003463", "RECOVER_Adult", "DigitalHealthData"]
) {
    /**
     * Configuration for a single variable/column.
     */
    public record VariableConfig(
        String column,      // Parquet column name
        String label,       // Human-readable label for concept path
        String forceType    // "NUMERIC", "TEXT", or null for auto-detect
    ) {}

    /**
     * Gets list of column names.
     */
    public List<String> variableColumns() {
        return variables.stream().map(VariableConfig::column).toList();
    }

    /**
     * Gets list of labels.
     */
    public List<String> variableLabels() {
        return variables.stream().map(VariableConfig::label).toList();
    }

    /**
     * Gets the variable config for a specific column.
     */
    public VariableConfig getVariableConfig(String column) {
        return variables.stream()
            .filter(v -> v.column.equals(column))
            .findFirst()
            .orElse(null);
    }
}

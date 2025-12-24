package edu.harvard.hms.dbmi.avillach.hpds.ingest.mapping;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Generic patient ID resolver using a configurable two-step mapping chain.
 *
 * Supports short-circuit resolution:
 * 1. If participantId parses as integer → use as patientNum directly
 * 2. Otherwise, run two-step mapping chain:
 *    - Step 1: participantId → intermediateId (e.g., dbgap_subject_id)
 *    - Step 2: intermediateId → patientNum
 */
public class MappingChainPatientIdResolver implements PatientIdResolver {
    private static final Logger log = LoggerFactory.getLogger(MappingChainPatientIdResolver.class);

    private final Map<String, String> step1Mapping;  // participantId -> intermediateId
    private final Map<String, Integer> step2Mapping; // intermediateId -> patientNum
    private final String step1Description;
    private final String step2Description;
    private final String step1File;
    private final String step2File;

    /**
     * Configuration for a two-step mapping chain.
     */
    public record ChainConfig(
        DelimitedFileMappingLoader.LoaderConfig step1Config,
        DelimitedFileMappingLoader.LoaderConfig step2Config,
        String step1Description,  // e.g., "participantId -> dbgap_subject_id"
        String step2Description   // e.g., "dbgap_subject_id -> patientNum"
    ) {}

    public MappingChainPatientIdResolver(ChainConfig config) throws IOException {
        this.step1Description = config.step1Description;
        this.step2Description = config.step2Description;
        this.step1File = config.step1Config.filePath().toAbsolutePath().toString();
        this.step2File = config.step2Config.filePath().toAbsolutePath().toString();

        log.info("Loading mapping chain step 1: {}", step1Description);
        this.step1Mapping = DelimitedFileMappingLoader.loadMapping(config.step1Config);

        log.info("Loading mapping chain step 2: {}", step2Description);
        Map<String, String> step2StringMap = DelimitedFileMappingLoader.loadMapping(config.step2Config);
        this.step2Mapping = convertToIntegerMap(step2StringMap, config.step2Config.filePath());

        // Validate patient numbers
        validatePatientNumbers(this.step2Mapping, config.step2Config.filePath());

        log.info("Loaded mapping chain: {} -> {}", step1Description, step2Description);
    }

    @Override
    public ResolutionResult resolvePatientNum(String participantId) {
        if (participantId == null || participantId.isBlank()) {
            return ResolutionResult.failure("INVALID_PARTICIPANT_ID", "Participant ID is null or empty");
        }

        // Short-circuit: if participantId is already an integer, use it directly
        Integer directPatientNum = tryParseInteger(participantId);
        if (directPatientNum != null) {
            if (directPatientNum <= 0) {
                return ResolutionResult.failure(
                    "INVALID_PATIENT_NUM_DIRECT",
                    "Participant ID parses as non-positive integer: " + directPatientNum
                );
            }
            log.debug("Short-circuit: participantId '{}' used directly as patientNum", participantId);
            return ResolutionResult.success(directPatientNum, null);
        }

        // Step 1: participantId -> intermediateId
        String intermediateId = step1Mapping.get(participantId);
        if (intermediateId == null) {
            return ResolutionResult.failure(
                "MISSING_STEP1_MAPPING",
                String.format("No mapping found in step 1 (%s) for participant: %s", step1Description, participantId)
            );
        }

        // Step 2: intermediateId -> patientNum
        Integer patientNum = step2Mapping.get(intermediateId);
        if (patientNum == null) {
            return ResolutionResult.failure(
                "MISSING_STEP2_MAPPING",
                String.format("No mapping found in step 2 (%s) for intermediate ID: %s", step2Description, intermediateId),
                intermediateId
            );
        }

        return ResolutionResult.success(patientNum, intermediateId);
    }

    @Override
    public MappingStatistics getStatistics() {
        return new MappingStatistics(
            step1Mapping.size(),
            step2Mapping.size(),
            step1File,
            step2File
        );
    }

    /**
     * Convert string map to integer map, validating all values parse as integers.
     */
    private Map<String, Integer> convertToIntegerMap(Map<String, String> stringMap, Path sourceFile) throws IOException {
        Map<String, Integer> intMap = new java.util.HashMap<>();

        for (Map.Entry<String, String> entry : stringMap.entrySet()) {
            try {
                int value = Integer.parseInt(entry.getValue());
                intMap.put(entry.getKey(), value);
            } catch (NumberFormatException e) {
                throw new IOException(String.format(
                    "Invalid patient number '%s' for key '%s' in %s",
                    entry.getValue(), entry.getKey(), sourceFile
                ));
            }
        }

        return intMap;
    }

    /**
     * Validate patient numbers are positive and warn about duplicates.
     */
    private void validatePatientNumbers(Map<String, Integer> mapping, Path sourceFile) throws IOException {
        Set<Integer> seenPatientNums = new HashSet<>();
        int minPatientNum = Integer.MAX_VALUE;
        int maxPatientNum = Integer.MIN_VALUE;
        int duplicateCount = 0;

        for (Map.Entry<String, Integer> entry : mapping.entrySet()) {
            int patientNum = entry.getValue();

            // Check positive
            if (patientNum <= 0) {
                throw new IOException(String.format(
                    "Invalid patient number %d for key %s in %s (must be positive)",
                    patientNum, entry.getKey(), sourceFile
                ));
            }

            // Check reasonable range
            if (patientNum > 100_000_000) {
                log.warn("Unusually large patient number {} for key {} in {}",
                    patientNum, entry.getKey(), sourceFile);
            }

            // Track duplicates
            if (!seenPatientNums.add(patientNum)) {
                duplicateCount++;
                if (duplicateCount <= 5) {
                    log.warn("Duplicate patient number {} found (key: {})", patientNum, entry.getKey());
                }
            }

            minPatientNum = Math.min(minPatientNum, patientNum);
            maxPatientNum = Math.max(maxPatientNum, patientNum);
        }

        if (duplicateCount > 0) {
            log.warn("Found {} duplicate patient numbers in {} - may indicate upstream mapping errors",
                duplicateCount, sourceFile);
        }

        log.info("Patient number validation: {} unique values, range [{}, {}]",
            seenPatientNums.size(), minPatientNum, maxPatientNum);
    }

    /**
     * Attempt to parse string as integer.
     */
    private Integer tryParseInteger(String value) {
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }
}

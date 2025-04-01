package edu.harvard.hms.dbmi.avillach.hpds.etl.phenotype.config;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;
import java.util.Optional;

public class ConfigLoader {

    private static final Logger log = LoggerFactory.getLogger(ConfigLoader.class);
    private static final String INPUT_DIR = "/opt/local/hpds_input";
    private Map<String, CSVConfig> csvConfigMap;

    /**
     * Constructor for ConfigLoader. It loads the configuration from a JSON file located in the INPUT_DIR.
     * The JSON file should contain a mapping of CSV file names to their respective configurations.
     */
    public ConfigLoader() {
        File file = new File(INPUT_DIR);
        if (file.exists()) {
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                this.csvConfigMap = objectMapper.readValue(new File(INPUT_DIR + "/config.json"), new TypeReference<>() {
                });

                // Remove the ".csv" extension from the keys in the map if they exist
                for (String key : csvConfigMap.keySet()) {
                    if (key.endsWith(".csv")) {
                        String newKey = key.substring(0, key.length() - 4);
                        csvConfigMap.put(newKey, csvConfigMap.get(key));
                        csvConfigMap.remove(key);
                    }
                }

                log.info("Loaded config from {}", file.getAbsolutePath());
                log.info("CSV Config Map: {}", csvConfigMap);
            } catch (Exception e) {
                log.error(e.getMessage());
            }
        } else {
            log.error("ConfigLoader: Input directory does not exist: {}", INPUT_DIR);
        }
    }

    /**
     * Checks if the config for the given csvName exists in the csvConfigMap.
     * @param csvName The name of the CSV file (without .csv extension)
     * @return true if the config exists, false otherwise
     */
    public boolean hasConfigFor(String csvName) {
        if (csvName == null || csvName.isEmpty()) {
            log.error("ConfigLoader: CSV name is null or empty");
            return false;
        }

        if (csvName.endsWith(".csv")) {
            csvName = csvName.substring(0, csvName.length() - 4);
        }

        if (csvConfigMap == null) {
            log.error("ConfigLoader: csvConfigMap is null");
            return false;
        }

        if (csvConfigMap.containsKey(csvName)) {
            log.info("ConfigLoader: Found config for csv name {}", csvName);
            return true;
        } else {
            log.error("ConfigLoader: No config found for csv name {}", csvName);
            return false;
        }
    }

    /**
     * Returns the CSVConfig for the given csvName. If the config does not exist, it returns an empty Optional.
     *
     * @param csvName The name of the CSV file (without .csv extension)
     * @return An Optional containing the CSVConfig if it exists, or an empty Optional if it does not
     */
    public Optional<CSVConfig> getConfigFor(String csvName) {
        // strip the .csv extension if it exists
        if (csvName == null || csvName.isEmpty()) {
            log.error("ConfigLoader: CSV name is null or empty");
            return Optional.empty();
        }

        if (csvName.endsWith(".csv")) {
            csvName = csvName.substring(0, csvName.length() - 4);
        }
        return hasConfigFor(csvName) ? Optional.of(csvConfigMap.get(csvName)) : Optional.empty();
    }

    /**
     * Returns all the CSV configurations loaded from the config.json file.
     * @return A map of CSV file names to their respective configurations
     */
    public Map<String, CSVConfig> getAllConfigs() {
        return csvConfigMap;
    }

}

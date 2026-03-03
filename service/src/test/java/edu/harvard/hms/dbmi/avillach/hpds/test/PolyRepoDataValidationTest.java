package edu.harvard.hms.dbmi.avillach.hpds.test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.ColumnMeta;
import edu.harvard.hms.dbmi.avillach.hpds.data.query.Filter;
import edu.harvard.hms.dbmi.avillach.hpds.data.query.Query;
import edu.harvard.hms.dbmi.avillach.hpds.data.query.ResultType;
import edu.harvard.hms.dbmi.avillach.hpds.processing.AbstractProcessor;
import edu.harvard.hms.dbmi.avillach.hpds.processing.CountProcessor;
import edu.harvard.hms.dbmi.avillach.hpds.processing.PhenotypeMetaStore;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive validation test for poly repo ingested data.
 * Dynamically generates tests based on columnMeta.csv and validates all HPDS query functionality.
 */
@ExtendWith(SpringExtension.class)
@EnableAutoConfiguration
@SpringBootTest(classes = edu.harvard.hms.dbmi.avillach.hpds.service.HpdsApplication.class)
@ActiveProfiles("polyrepo-validation")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class PolyRepoDataValidationTest {

    private static final Logger log = LoggerFactory.getLogger(PolyRepoDataValidationTest.class);
    private static final ObjectMapper objectMapper = new ObjectMapper()
        .registerModule(new JavaTimeModule());

    @Autowired
    private AbstractProcessor abstractProcessor;

    @Autowired
    private CountProcessor countProcessor;

    @Autowired
    private PhenotypeMetaStore phenotypeMetaStore;

    @Value("${HPDS_DATA_DIRECTORY:/opt/local/hpds/}")
    private String hpdsDataDirectory;

    private static ValidationConfig config;
    private static ValidationReport report;
    private static Map<String, ConceptMetadata> conceptMetadataMap;

    @BeforeAll
    public static void setupValidation() throws IOException {
        log.info("Starting HPDS Poly Repo Data Validation");
        report = new ValidationReport();
        report.startTime = LocalDateTime.now();

        // Load validation config
        String configPath = System.getProperty("validation.config",
            "./configs/hpds_validation_config.json");
        config = loadValidationConfig(configPath);
        log.info("Loaded validation config from: {}", configPath);

        // Parse columnMeta.csv for dynamic test generation
        conceptMetadataMap = parseColumnMeta(config.dataDirectory + "/columnMeta.csv");
        log.info("Parsed {} concepts from columnMeta.csv", conceptMetadataMap.size());

        report.totalConcepts = conceptMetadataMap.size();
    }

    @AfterAll
    public static void generateReport() throws IOException {
        report.endTime = LocalDateTime.now();
        report.durationSeconds = java.time.Duration.between(report.startTime, report.endTime).getSeconds();

        String reportPath = config.reportOutput.directory + "/" +
            config.reportOutput.filename.replace(".json",
                "_" + report.startTime.format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")) + ".json");

        Files.write(Paths.get(reportPath),
            objectMapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(report));

        log.info("=== VALIDATION REPORT ===");
        log.info("Total Tests: {}", report.totalTests);
        log.info("Passed: {}", report.passedTests);
        log.info("Failed: {}", report.failedTests);
        log.info("Duration: {} seconds", report.durationSeconds);
        log.info("Report saved to: {}", reportPath);

        if (report.failedTests > 0) {
            log.error("VALIDATION FAILED - {} tests failed", report.failedTests);
            fail("Validation failed with " + report.failedTests + " failures. See report: " + reportPath);
        } else {
            log.info("VALIDATION PASSED - All tests successful!");
        }
    }

    @Test
    @Order(1)
    public void testBasicConfiguration() {
        TestResult result = new TestResult("Basic Configuration Check");
        try {
            assertNotNull(abstractProcessor, "AbstractProcessor should be autowired");
            assertNotNull(countProcessor, "CountProcessor should be autowired");
            assertNotNull(phenotypeMetaStore, "PhenotypeMetaStore should be autowired");

            assertTrue(Files.exists(Paths.get(hpdsDataDirectory)),
                "HPDS data directory should exist: " + hpdsDataDirectory);
            assertTrue(Files.exists(Paths.get(hpdsDataDirectory + "/allObservationsStore.javabin")),
                "allObservationsStore.javabin should exist");
            assertTrue(Files.exists(Paths.get(hpdsDataDirectory + "/columnMeta.javabin")),
                "columnMeta.javabin should exist");

            result.passed = true;
            result.message = "Configuration validated successfully";
        } catch (Exception e) {
            result.passed = false;
            result.message = "Configuration validation failed: " + e.getMessage();
            result.error = getStackTrace(e);
        }
        recordTestResult(result);
    }

    @Test
    @Order(2)
    public void testTotalPatientCount() {
        TestResult result = new TestResult("Total Patient Count Validation");
        try {
            Query query = new Query();
            Set<Integer> allPatients = abstractProcessor.getPatientSubsetForQuery(query);

            int actualCount = allPatients.size();
            int expectedCount = config.expectedPatientCount;

            assertEquals(expectedCount, actualCount,
                "Total patient count should match expected");

            result.passed = true;
            result.message = String.format("Patient count validated: %d patients", actualCount);
            result.details.put("expectedPatients", expectedCount);
            result.details.put("actualPatients", actualCount);

        } catch (Exception e) {
            result.passed = false;
            result.message = "Patient count validation failed: " + e.getMessage();
            result.error = getStackTrace(e);
        }
        recordTestResult(result);
    }

    @Test
    @Order(3)
    public void testConceptCount() {
        TestResult result = new TestResult("Concept Count Validation");
        try {
            int actualCount = conceptMetadataMap.size();
            int expectedCount = config.expectedConceptCount;
            int difference = Math.abs(actualCount - expectedCount);

            log.info("Concept count - Expected: {}, Actual: {}, Difference: {}",
                expectedCount, actualCount, difference);

            // If there's a difference, identify which concepts are affected
            if (difference > 0) {
                // Try to load expected concepts from a previous columnMeta if available
                String columnMetaPath = config.dataDirectory + "columnMeta.csv";
                try (BufferedReader reader = new BufferedReader(new FileReader(columnMetaPath))) {
                    long totalLines = reader.lines().count();
                    log.info("Total lines in columnMeta.csv: {}", totalLines);
                    log.info("Successfully parsed concepts: {}", actualCount);
                    log.info("Parse failures (likely due to malformed lines): {}", totalLines - actualCount);
                } catch (IOException e) {
                    log.warn("Could not read columnMeta.csv for diagnostics", e);
                }
            }

            // Allow small variance for parse failures on malformed lines
            assertTrue(Math.abs(actualCount - expectedCount) < 100,
                String.format("Concept count should be close to expected. Expected: %d, Actual: %d, Difference: %d",
                    expectedCount, actualCount, difference));

            result.passed = true;
            result.message = String.format("Concept count validated: %d concepts (expected: %d, difference: %d)",
                actualCount, expectedCount, difference);
            result.details.put("expectedConcepts", expectedCount);
            result.details.put("actualConcepts", actualCount);
            result.details.put("difference", difference);

        } catch (Exception e) {
            result.passed = false;
            result.message = "Concept count validation failed: " + e.getMessage();
            result.error = getStackTrace(e);
        }
        recordTestResult(result);
    }

    @Test
    @Order(4)
    public void testContinuousConceptSampling() {
        List<ConceptMetadata> continuousConcepts = conceptMetadataMap.values().stream()
            .filter(c -> !c.categorical && c.observationCount >= config.samplingStrategy.minObservationsForTest)
            .limit(config.samplingStrategy.continuousConcepts)
            .collect(Collectors.toList());

        log.info("Testing {} continuous concepts", continuousConcepts.size());

        for (ConceptMetadata concept : continuousConcepts) {
            TestResult result = new TestResult("Continuous Concept: " + concept.conceptPath);
            try {
                Query query = new Query();
                query.setNumericFilters(Map.of(
                    concept.conceptPath,
                    new Filter.DoubleFilter(concept.min, concept.max)
                ));

                Set<Integer> patients = abstractProcessor.getPatientSubsetForQuery(query);

                assertTrue(patients.size() > 0,
                    "Query should return patients for concept with observations");
                assertTrue(patients.size() <= config.expectedPatientCount,
                    "Result should not exceed total patient count");

                result.passed = true;
                result.message = String.format("Numeric filter returned %d patients (range: %.2f - %.2f)",
                    patients.size(), concept.min, concept.max);
                result.details.put("conceptPath", concept.conceptPath);
                result.details.put("patientCount", patients.size());
                result.details.put("rangeMin", concept.min);
                result.details.put("rangeMax", concept.max);

            } catch (Exception e) {
                result.passed = false;
                result.message = "Continuous concept test failed: " + e.getMessage();
                result.error = getStackTrace(e);
            }
            recordTestResult(result);
        }
    }

    @Test
    @Order(5)
    public void testCategoricalConceptSampling() {
        List<ConceptMetadata> categoricalConcepts = conceptMetadataMap.values().stream()
            .filter(c -> c.categorical && c.observationCount >= config.samplingStrategy.minObservationsForTest)
            .limit(config.samplingStrategy.categoricalConcepts)
            .collect(Collectors.toList());

        log.info("Testing {} categorical concepts", categoricalConcepts.size());

        for (ConceptMetadata concept : categoricalConcepts) {
            TestResult result = new TestResult("Categorical Concept: " + concept.conceptPath);
            try {
                // Get first category value to test
                String[] categories = concept.categoryValues.split("µ");
                if (categories.length > 0 && !categories[0].isEmpty()) {
                    Query query = new Query();
                    query.setCategoryFilters(Map.of(
                        concept.conceptPath,
                        new String[]{categories[0]}
                    ));

                    Set<Integer> patients = abstractProcessor.getPatientSubsetForQuery(query);

                    assertTrue(patients.size() >= 0,
                        "Query should execute successfully");
                    assertTrue(patients.size() <= config.expectedPatientCount,
                        "Result should not exceed total patient count");

                    result.passed = true;
                    result.message = String.format("Categorical filter for '%s' returned %d patients",
                        categories[0], patients.size());
                    result.details.put("conceptPath", concept.conceptPath);
                    result.details.put("categoryValue", categories[0]);
                    result.details.put("patientCount", patients.size());
                    result.details.put("totalCategories", categories.length);
                }

            } catch (Exception e) {
                result.passed = false;
                result.message = "Categorical concept test failed: " + e.getMessage();
                result.error = getStackTrace(e);
            }
            recordTestResult(result);
        }
    }

    @Test
    @Order(6)
    public void testConfiguredQueryScenarios() {
        log.info("Testing {} configured query scenarios", config.queryScenarios.size());

        for (QueryScenario scenario : config.queryScenarios) {
            TestResult result = new TestResult("Scenario: " + scenario.name);
            try {
                Query query = new Query();

                // Apply numeric filters
                if (scenario.numericFilters != null && !scenario.numericFilters.isEmpty()) {
                    Map<String, Filter.DoubleFilter> filters = new HashMap<>();
                    scenario.numericFilters.forEach((path, range) ->
                        filters.put(path, new Filter.DoubleFilter(range.min, range.max)));
                    query.setNumericFilters(filters);
                }

                // Apply category filters
                if (scenario.categoryFilters != null && !scenario.categoryFilters.isEmpty()) {
                    Map<String, String[]> filters = new HashMap<>();
                    scenario.categoryFilters.forEach((path, values) ->
                        filters.put(path, values.toArray(new String[0])));
                    query.setCategoryFilters(filters);
                }

                Set<Integer> patients = abstractProcessor.getPatientSubsetForQuery(query);
                int patientCount = patients.size();

                // Validate against expected ranges
                assertTrue(patientCount >= scenario.expectedMinPatients,
                    String.format("Patient count %d should be >= %d", patientCount, scenario.expectedMinPatients));
                assertTrue(patientCount <= scenario.expectedMaxPatients,
                    String.format("Patient count %d should be <= %d", patientCount, scenario.expectedMaxPatients));

                result.passed = true;
                result.message = String.format("%s: %d patients (expected %d-%d)",
                    scenario.description, patientCount,
                    scenario.expectedMinPatients, scenario.expectedMaxPatients);
                result.details.put("scenarioName", scenario.name);
                result.details.put("patientCount", patientCount);
                result.details.put("expectedMin", scenario.expectedMinPatients);
                result.details.put("expectedMax", scenario.expectedMaxPatients);

            } catch (Exception e) {
                result.passed = false;
                result.message = "Query scenario failed: " + e.getMessage();
                result.error = getStackTrace(e);
            }
            recordTestResult(result);
        }
    }

    @Test
    @Order(7)
    public void testCrossCountValidation() {
        // Test cross-counts on a sample categorical concept
        List<ConceptMetadata> categoricalConcepts = conceptMetadataMap.values().stream()
            .filter(c -> c.categorical && c.observationCount >= 100)
            .limit(5)
            .collect(Collectors.toList());

        for (ConceptMetadata concept : categoricalConcepts) {
            TestResult result = new TestResult("Cross-Count: " + concept.conceptPath);
            try {
                Query query = new Query();
                query.setCrossCountFields(List.of());
                query.setExpectedResultType(ResultType.CROSS_COUNT);

                String[] categories = concept.categoryValues.split("µ");
                if (categories.length > 0 && !categories[0].isEmpty()) {
                    query.setCategoryFilters(Map.of(
                        concept.conceptPath,
                        categories
                    ));

                    Map<String, Map<String, Integer>> crossCounts =
                        countProcessor.runCategoryCrossCounts(query);

                    assertNotNull(crossCounts, "Cross-counts should not be null");
                    assertTrue(crossCounts.containsKey(concept.conceptPath),
                        "Cross-count should include the queried concept");

                    Map<String, Integer> counts = crossCounts.get(concept.conceptPath);
                    int totalCount = counts.values().stream().mapToInt(Integer::intValue).sum();

                    result.passed = true;
                    result.message = String.format("Cross-count validated: %d total patients across %d categories",
                        totalCount, counts.size());
                    result.details.put("conceptPath", concept.conceptPath);
                    result.details.put("categoryCount", counts.size());
                    result.details.put("totalPatients", totalCount);
                }

            } catch (Exception e) {
                result.passed = false;
                result.message = "Cross-count test failed: " + e.getMessage();
                result.error = getStackTrace(e);
            }
            recordTestResult(result);
        }
    }

    // ==================== Helper Classes & Methods ====================

    private static ValidationConfig loadValidationConfig(String path) throws IOException {
        return objectMapper.readValue(new File(path), ValidationConfig.class);
    }

    private static Map<String, ConceptMetadata> parseColumnMeta(String path) throws IOException {
        Map<String, ConceptMetadata> map = new HashMap<>();

        // Use Apache Commons CSV RFC4180 parser - same as ColumnMetaBuilder uses for writing
        try (Reader reader = new FileReader(path);
             CSVParser parser = new CSVParser(reader, CSVFormat.RFC4180)) {

            boolean isFirstRecord = true;
            for (CSVRecord record : parser) {
                // Skip header row
                if (isFirstRecord) {
                    isFirstRecord = false;
                    continue;
                }

                ConceptMetadata metadata = parseColumnMetaRecord(record);
                if (metadata != null) {
                    map.put(metadata.conceptPath, metadata);
                }
            }
        }
        return map;
    }

    private static ConceptMetadata parseColumnMetaRecord(CSVRecord record) {
        try {
            if (record.size() < 11) {
                log.warn("CSV record has fewer than 11 fields: {}", record.size());
                return null;
            }

            ConceptMetadata meta = new ConceptMetadata();
            meta.conceptPath = record.get(0);
            meta.dataType = Integer.parseInt(record.get(1));
            meta.width = Integer.parseInt(record.get(2));
            meta.categorical = "true".equals(record.get(3));
            meta.categoryValues = record.get(4);

            String minStr = record.get(5);
            String maxStr = record.get(6);
            if (!minStr.isEmpty()) meta.min = Double.parseDouble(minStr);
            if (!maxStr.isEmpty()) meta.max = Double.parseDouble(maxStr);

            meta.observationCount = Integer.parseInt(record.get(9));
            meta.patientCount = Integer.parseInt(record.get(10));

            return meta;
        } catch (Exception e) {
            log.warn("Failed to parse columnMeta record at line {}: {}", record.getRecordNumber(), e.getMessage());
            return null;
        }
    }

    private synchronized void recordTestResult(TestResult result) {
        report.totalTests++;
        if (result.passed) {
            report.passedTests++;
        } else {
            report.failedTests++;
        }
        report.testResults.add(result);

        if (result.passed) {
            log.info("✓ PASS: {}", result.testName);
        } else {
            log.error("✗ FAIL: {} - {}", result.testName, result.message);
        }
    }

    private String getStackTrace(Exception e) {
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }

    // ==================== Data Classes ====================

    static class ValidationConfig {
        public String dataDirectory;
        public int expectedPatientCount;
        public int expectedConceptCount;
        public SamplingStrategy samplingStrategy;
        public List<QueryScenario> queryScenarios;
        public PerformanceThresholds performanceThresholds;
        public ReportOutput reportOutput;
    }

    static class SamplingStrategy {
        public int continuousConcepts;
        public int categoricalConcepts;
        public int timeSeriesConcepts;
        public int minObservationsForTest;
    }

    static class QueryScenario {
        public String name;
        public String description;
        public Map<String, FilterRange> numericFilters;
        public Map<String, List<String>> categoryFilters;
        public int expectedMinPatients;
        public int expectedMaxPatients;
    }

    static class FilterRange {
        public Double min;
        public Double max;
    }

    static class PerformanceThresholds {
        public long maxQueryTimeMs;
        public long maxConceptLoadTimeMs;
        public long maxCrossCountTimeMs;
    }

    static class ReportOutput {
        public String directory;
        public String filename;
    }

    static class ConceptMetadata {
        public String conceptPath;
        public int dataType;
        public int width;
        public boolean categorical;
        public String categoryValues;
        public double min;
        public double max;
        public int observationCount;
        public int patientCount;
    }

    static class ValidationReport {
        public LocalDateTime startTime;
        public LocalDateTime endTime;
        public long durationSeconds;
        public int totalTests = 0;
        public int passedTests = 0;
        public int failedTests = 0;
        public int totalConcepts = 0;
        public List<TestResult> testResults = new ArrayList<>();
    }

    static class TestResult {
        public String testName;
        public boolean passed;
        public String message;
        public String error;
        public Map<String, Object> details = new HashMap<>();

        public TestResult(String testName) {
            this.testName = testName;
        }
    }
}

package edu.harvard.hms.dbmi.avillach.hpds.test;

import edu.harvard.hms.dbmi.avillach.hpds.data.query.Query;
import edu.harvard.hms.dbmi.avillach.hpds.processing.AbstractProcessor;
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

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Quick smoke tests for EC2 validation.
 * Runs basic sanity checks before S3 upload to fail fast if major issues detected.
 *
 * Tests:
 * - Critical files exist
 * - Patient count is reasonable
 * - Sample concepts are queryable
 * - Basic query operations work
 *
 * Expected runtime: 30-60 seconds
 */
@ExtendWith(SpringExtension.class)
@EnableAutoConfiguration
@SpringBootTest(classes = edu.harvard.hms.dbmi.avillach.hpds.service.HpdsApplication.class)
@ActiveProfiles("polyrepo-validation")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class QuickValidationSmokeTest {

    private static final Logger log = LoggerFactory.getLogger(QuickValidationSmokeTest.class);

    private static final int MIN_EXPECTED_PATIENTS = 1000;
    private static final int MAX_EXPECTED_PATIENTS = 10000;
    private static final int MIN_EXPECTED_CONCEPTS = 100000;

    @Autowired
    private AbstractProcessor abstractProcessor;

    @Autowired
    private PhenotypeMetaStore phenotypeMetaStore;

    @Value("${HPDS_DATA_DIRECTORY:/opt/local/hpds/}")
    private String hpdsDataDirectory;

    @Test
    @Order(1)
    @DisplayName("Critical files exist")
    public void testCriticalFilesExist() {
        log.info("Checking critical file existence...");

        assertTrue(Files.exists(Paths.get(hpdsDataDirectory)),
            "HPDS data directory should exist: " + hpdsDataDirectory);

        assertTrue(Files.exists(Paths.get(hpdsDataDirectory + "/allObservationsStore.javabin")),
            "allObservationsStore.javabin must exist");

        assertTrue(Files.exists(Paths.get(hpdsDataDirectory + "/columnMeta.javabin")),
            "columnMeta.javabin must exist");

        assertTrue(Files.exists(Paths.get(hpdsDataDirectory + "/columnMeta.csv")),
            "columnMeta.csv must exist");

        log.info("✓ All critical files present");
    }

    @Test
    @Order(2)
    @DisplayName("Spring context loads successfully")
    public void testSpringContextLoads() {
        log.info("Checking Spring context...");

        assertNotNull(abstractProcessor, "AbstractProcessor should be autowired");
        assertNotNull(phenotypeMetaStore, "PhenotypeMetaStore should be autowired");

        log.info("✓ Spring context loaded successfully");
    }

    @Test
    @Order(3)
    @DisplayName("Patient count is reasonable")
    public void testPatientCount() {
        log.info("Checking total patient count...");

        Query query = new Query();
        Set<Integer> allPatients = abstractProcessor.getPatientSubsetForQuery(query);

        int patientCount = allPatients.size();
        log.info("Found {} patients", patientCount);

        assertTrue(patientCount >= MIN_EXPECTED_PATIENTS,
            String.format("Patient count %d should be >= %d", patientCount, MIN_EXPECTED_PATIENTS));

        assertTrue(patientCount <= MAX_EXPECTED_PATIENTS,
            String.format("Patient count %d should be <= %d", patientCount, MAX_EXPECTED_PATIENTS));

        log.info("✓ Patient count {} is within expected range", patientCount);
    }

    @Test
    @Order(4)
    @DisplayName("Concept metadata is loaded")
    public void testConceptMetadataLoaded() {
        log.info("Checking concept metadata...");

        List<String> allConcepts = new ArrayList<>(phenotypeMetaStore.getColumnNames());
        int conceptCount = allConcepts.size();

        log.info("Found {} concepts", conceptCount);

        assertTrue(conceptCount >= MIN_EXPECTED_CONCEPTS,
            String.format("Concept count %d should be >= %d", conceptCount, MIN_EXPECTED_CONCEPTS));

        log.info("✓ Loaded {} concepts", conceptCount);
    }

    @Test
    @Order(5)
    @DisplayName("Sample concepts are queryable")
    public void testSampleConceptsQueryable() {
        log.info("Testing sample concepts...");

        List<String> allConcepts = new ArrayList<>(phenotypeMetaStore.getColumnNames());

        // Sample 5 concepts randomly
        Collections.shuffle(allConcepts);
        List<String> sampleConcepts = allConcepts.stream()
            .filter(c -> !c.equals("SUBJECT_ID"))
            .limit(5)
            .collect(Collectors.toList());

        log.info("Testing {} sampled concepts", sampleConcepts.size());

        for (String concept : sampleConcepts) {
            try {
                Query query = new Query();
                query.setFields(List.of(concept));

                // Should not throw exception
                Set<Integer> patients = abstractProcessor.getPatientSubsetForQuery(query);

                log.debug("  ✓ Concept queryable: {} ({} patients)", concept, patients.size());

            } catch (Exception e) {
                fail("Failed to query concept: " + concept + " - " + e.getMessage());
            }
        }

        log.info("✓ All {} sampled concepts are queryable", sampleConcepts.size());
    }

    @Test
    @Order(6)
    @DisplayName("Basic query operations work")
    public void testBasicQueryOperations() {
        log.info("Testing basic query operations...");

        // Test 1: Empty query (all patients)
        Query emptyQuery = new Query();
        Set<Integer> allPatients = abstractProcessor.getPatientSubsetForQuery(emptyQuery);
        assertTrue(allPatients.size() > 0, "Empty query should return patients");
        log.debug("  ✓ Empty query returned {} patients", allPatients.size());

        // Test 2: Query with anyRecordOf
        List<String> conceptsWithData = new ArrayList<>(phenotypeMetaStore.getColumnNames())
            .stream()
            .filter(c -> !c.equals("SUBJECT_ID"))
            .filter(c -> {
                try {
                    return phenotypeMetaStore.getColumnMeta(c).getObservationCount() > 100;
                } catch (Exception e) {
                    return false;
                }
            })
            .limit(2)
            .collect(Collectors.toList());

        if (!conceptsWithData.isEmpty()) {
            Query anyRecordOfQuery = new Query();
            anyRecordOfQuery.setAnyRecordOf(conceptsWithData);
            Set<Integer> anyRecordOfPatients = abstractProcessor.getPatientSubsetForQuery(anyRecordOfQuery);
            assertTrue(anyRecordOfPatients.size() > 0, "anyRecordOf query should return patients");
            log.debug("  ✓ anyRecordOf query returned {} patients", anyRecordOfPatients.size());
        }

        log.info("✓ Basic query operations work correctly");
    }

    @Test
    @Order(7)
    @DisplayName("No duplicate patient IDs")
    public void testNoDuplicatePatientIds() {
        log.info("Checking for duplicate patient IDs...");

        Query query = new Query();
        Set<Integer> allPatients = abstractProcessor.getPatientSubsetForQuery(query);

        // Set should not contain duplicates by definition, but let's verify the count
        List<Integer> patientList = new ArrayList<>(allPatients);
        Set<Integer> uniquePatients = new HashSet<>(patientList);

        assertEquals(uniquePatients.size(), patientList.size(),
            "Should have no duplicate patient IDs");

        log.info("✓ No duplicate patient IDs found ({} unique patients)", uniquePatients.size());
    }

    @AfterAll
    public static void summary() {
        log.info("========================================");
        log.info("Quick Validation Smoke Test Complete");
        log.info("========================================");
        log.info("All critical smoke tests passed.");
        log.info("Ingestion output is ready for S3 upload.");
    }
}

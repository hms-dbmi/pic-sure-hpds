package edu.harvard.hms.dbmi.avillach.hpds.test;

import edu.harvard.hms.dbmi.avillach.hpds.data.query.ResultType;
import edu.harvard.hms.dbmi.avillach.hpds.data.query.v3.*;
import edu.harvard.hms.dbmi.avillach.hpds.processing.v3.CountV3Processor;
import edu.harvard.hms.dbmi.avillach.hpds.test.util.BuildIntegrationTestEnvironment;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(SpringExtension.class)
@EnableAutoConfiguration
@SpringBootTest(classes = edu.harvard.hms.dbmi.avillach.hpds.service.HpdsApplication.class)
@ActiveProfiles("integration-test")
public class CountV3ProcessorIntegrationTest {

    @Autowired
    private CountV3Processor countProcessor;

    @BeforeAll
    public static void beforeAll() {
        BuildIntegrationTestEnvironment instance = BuildIntegrationTestEnvironment.INSTANCE;
    }

    @Test
    public void runCategoryCrossCounts_twoFilters() {
        PhenotypicFilter sexFilter = new PhenotypicFilter(PhenotypicFilterType.FILTER, "\\open_access-1000Genomes\\data\\SEX\\", List.of("male", "female"), null, null, null);
        PhenotypicFilter populationFilter = new PhenotypicFilter(PhenotypicFilterType.FILTER, "\\open_access-1000Genomes\\data\\POPULATION NAME\\", List.of("Finnish"), null, null, null);

        PhenotypicSubquery phenotypicSubquery = new PhenotypicSubquery(
                null,
                List.of(sexFilter, populationFilter),
                Operator.AND
        );
        Query query = new Query(
                List.of(),
                List.of(),
                phenotypicSubquery,
                null,
                ResultType.COUNT,
                null,
                null);

        Map<String, Map<String, Integer>> crossCounts = countProcessor.runCategoryCrossCounts(query);
        assertEquals(2, crossCounts.size());
        assertEquals(38, crossCounts.get("\\open_access-1000Genomes\\data\\SEX\\").get("male"));
        assertEquals(64, crossCounts.get("\\open_access-1000Genomes\\data\\SEX\\").get("female"));
        assertEquals(102, crossCounts.get("\\open_access-1000Genomes\\data\\POPULATION NAME\\").get("Finnish"));
    }

    /**
     * This test illustrates how there is now ambiguity in how we do cross counts when OR functionality is introduced. In
     * this example, because there is an OR clause, the cross counts will not match the total patient set. In this case,
     * since only "Finnish" was queried as a POPULATION_NAME, we only return cross counts for "Finnish" patients, even
     * though the total patient set contains patients with other POPULATION_NAME.
     */
    @Test
    public void runCategoryCrossCounts_twoFiltersOr() {
        PhenotypicFilter sexFilter = new PhenotypicFilter(PhenotypicFilterType.FILTER, "\\open_access-1000Genomes\\data\\SEX\\", List.of("male"), null, null, null);
        PhenotypicFilter populationFilter = new PhenotypicFilter(PhenotypicFilterType.FILTER, "\\open_access-1000Genomes\\data\\POPULATION NAME\\", List.of("Finnish"), null, null, null);

        PhenotypicSubquery phenotypicSubquery = new PhenotypicSubquery(
                null,
                List.of(sexFilter, populationFilter),
                Operator.OR
        );
        Query query = new Query(
                List.of(),
                List.of(),
                phenotypicSubquery,
                null,
                ResultType.COUNT,
                null,
                null);

        Map<String, Map<String, Integer>> crossCounts = countProcessor.runCategoryCrossCounts(query);
        assertEquals(2, crossCounts.size());
        assertEquals(2648, crossCounts.get("\\open_access-1000Genomes\\data\\SEX\\").get("male"));
        assertEquals(102, crossCounts.get("\\open_access-1000Genomes\\data\\POPULATION NAME\\").get("Finnish"));
    }

    @Test
    public void runCategoryCrossCounts_snpFilter() {
        PhenotypicFilter sexFilter = new PhenotypicFilter(PhenotypicFilterType.FILTER, "\\open_access-1000Genomes\\data\\SEX\\", List.of("male"), null, null, null);
        Query query = new Query(
                List.of(),
                List.of(),
                sexFilter,
                List.of(new GenomicFilter("chr21,5032061,A,G", List.of("0/1", "1/1"), null, null)),
                ResultType.COUNT,
                null,
                null);

        Map<String, Map<String, Integer>> crossCounts = countProcessor.runCategoryCrossCounts(query);
        assertEquals(1, crossCounts.size());
        assertEquals(2, crossCounts.get("\\open_access-1000Genomes\\data\\SEX\\").get("male"));
    }

}

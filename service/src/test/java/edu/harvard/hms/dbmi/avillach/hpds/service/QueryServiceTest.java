package edu.harvard.hms.dbmi.avillach.hpds.service;

import edu.harvard.hms.dbmi.avillach.hpds.data.query.Query;
import edu.harvard.hms.dbmi.avillach.hpds.data.query.ResultType;
import edu.harvard.hms.dbmi.avillach.hpds.processing.AsyncResult;
import edu.harvard.hms.dbmi.avillach.hpds.test.util.BuildIntegrationTestEnvironment;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
@ExtendWith(SpringExtension.class)
@EnableAutoConfiguration
@SpringBootTest(classes = edu.harvard.hms.dbmi.avillach.hpds.service.HpdsApplication.class)
@ActiveProfiles("integration-test")
class QueryServiceTest {

    @Autowired
    private QueryService queryService;

    @BeforeAll
    public static void beforeAll() {
        BuildIntegrationTestEnvironment instance = BuildIntegrationTestEnvironment.INSTANCE;
    }

    @Test
    public void dataframeMulti() throws IOException, InterruptedException {
        Query query = new Query();
        List<Query.VariantInfoFilter> variantInfoFilters = new ArrayList<>();
        Query.VariantInfoFilter variantInfoFilter = new Query.VariantInfoFilter();
        variantInfoFilter.categoryVariantInfoFilters = Map.of("Gene_with_variant", new String[]{"LOC102723996", "LOC101928576"});
        variantInfoFilters.add(variantInfoFilter);
        query.setVariantInfoFilters(variantInfoFilters);
        query.setFields(List.of("\\open_access-1000Genomes\\data\\SYNTHETIC_AGE\\"));
        query.setExpectedResultType(ResultType.DATAFRAME_MULTI);

        AsyncResult asyncResult = queryService.runQuery(query);

        Thread.sleep(1000);

        System.out.println(asyncResult.getStatus());
        System.out.println(IOUtils.toString(new FileInputStream(asyncResult.getFile()), StandardCharsets.UTF_8));
        ;
    }

}
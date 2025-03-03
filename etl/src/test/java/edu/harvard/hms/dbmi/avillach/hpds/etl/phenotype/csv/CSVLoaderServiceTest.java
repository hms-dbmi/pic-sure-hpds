package edu.harvard.hms.dbmi.avillach.hpds.etl.phenotype.csv;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.io.IOException;

@SpringBootTest
@TestPropertySource(properties = {
        "encryption.enabled=false",
        "etl.hpds.directory=src/test/resources/"
})
class CSVLoaderServiceTest {

    @Autowired
    private CSVLoaderService service;

    @Test
    void testEtlWithTestCsv() throws IOException {
        service.runEtlProcess();
    }
}
package edu.harvard.hms.dbmi.avillach.hpds.test;

import edu.harvard.dbmi.avillach.domain.GeneralQueryRequest;
import edu.harvard.dbmi.avillach.domain.QueryRequest;
import edu.harvard.dbmi.avillach.domain.QueryStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static edu.harvard.dbmi.avillach.util.PicSureStatus.NOT_FOUND;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(SpringExtension.class)
@EnableAutoConfiguration
@SpringBootTest(
    classes = edu.harvard.hms.dbmi.avillach.hpds.service.HpdsApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT
)
@ActiveProfiles("integration-test")
public class PicSureV3ServiceIntegrationTest {
    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate testRestTemplate;

    @Test
    void queryStatus_invalidUUID_returnsNotFound() {
        ResponseEntity<QueryStatus> response = testRestTemplate.postForEntity(
            "http://localhost:" + port + "/PIC-SURE/v3/query/82eb6844-817c-4bc7-a148-49930879b825/status", new GeneralQueryRequest(),
            QueryStatus.class
        );

        assertEquals(NOT_FOUND, response.getBody().getStatus());
    }

    // todo: write some full end to end tests around submitting and retrieving queries
}

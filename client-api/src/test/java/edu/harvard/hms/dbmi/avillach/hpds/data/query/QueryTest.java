package edu.harvard.hms.dbmi.avillach.hpds.data.query;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.TreeSet;

class QueryTest {

    @Test
    void shouldPrintIncludedPatients() {
        Query q = new Query();
        q.setPatientIDQuery(new PatientIDQuery(InclusionRule.Include, new TreeSet<>(List.of(1, 2, 3))));

        String actual = q.toString();
        String expectedExcerpt = "Include Patient IDs: [3 IDs]";

        Assertions.assertTrue(actual.contains(expectedExcerpt));
    }

    @Test
    void shouldPrintExcludedPatients() {
        Query q = new Query();
        q.setPatientIDQuery(new PatientIDQuery(InclusionRule.Exclude, new TreeSet<>(List.of(1, 2, 3))));

        String actual = q.toString();
        String expectedExcerpt = "Exclude Patient IDs: [3 IDs]";

        Assertions.assertTrue(actual.contains(expectedExcerpt));
    }

    @Test
    void shouldPrintNullPatients() {
        Query q = new Query();
        q.setPatientIDQuery(null);

        String actual = q.toString();
        String expectedExcerpt = "Patient IDs: null";

        Assertions.assertTrue(actual.contains(expectedExcerpt));
    }

    @Test
    void shouldPrintExcludedNullPatientList() {
        Query q = new Query();
        q.setPatientIDQuery(new PatientIDQuery(InclusionRule.Only, null));

        String actual = q.toString();
        String expectedExcerpt = "Patient IDs: null";

        Assertions.assertTrue(actual.contains(expectedExcerpt));
    }
}

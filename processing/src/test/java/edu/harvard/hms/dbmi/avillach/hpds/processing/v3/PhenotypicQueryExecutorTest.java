package edu.harvard.hms.dbmi.avillach.hpds.processing.v3;

import edu.harvard.hms.dbmi.avillach.hpds.data.query.ResultType;
import edu.harvard.hms.dbmi.avillach.hpds.data.query.v3.*;
import edu.harvard.hms.dbmi.avillach.hpds.processing.PhenotypeMetaStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;


@ExtendWith(MockitoExtension.class)
class PhenotypicQueryExecutorTest {

    @Mock
    private PhenotypeMetaStore phenotypeMetaStore;

    @Mock
    private PhenotypicObservationStore phenotypicObservationStore;

    private PhenotypicQueryExecutor phenotypicQueryExecutor;

    @BeforeEach
    public void setup() {
        phenotypicQueryExecutor = new PhenotypicQueryExecutor(phenotypeMetaStore, phenotypicObservationStore);
    }

    @Test
    public void getPatientSet_noFilters_returnAllPatients() {
        Query query = new Query(List.of(), List.of(), null, null, ResultType.COUNT, null, null);

        Set<Integer> patientIds = Set.of(10, 100, 1000);
        when(phenotypeMetaStore.getPatientIds()).thenReturn(new TreeSet<>(patientIds));

        Set<Integer> patientSet = phenotypicQueryExecutor.getPatientSet(query);
        assertEquals(patientIds, patientSet);
    }

    @Test
    public void getPatientSet_validNumericFilter_returnPatients() throws ExecutionException {
        String conceptPath = "\\open_access-1000Genomes\\data\\SYNTHETIC_AGE\\";
        Query query = new Query(
            List.of(), List.of(), new PhenotypicFilter(PhenotypicFilterType.FILTER, conceptPath, null, 35.0, 45.0, null), null,
            ResultType.COUNT, null, null
        );

        Set<Integer> patientIds = Set.of(2, 3, 5);
        when(phenotypicObservationStore.getKeysForRange(conceptPath, 35.0, 45.0)).thenReturn(patientIds);

        Set<Integer> patientSet = phenotypicQueryExecutor.getPatientSet(query);
        assertEquals(patientIds, patientSet);
    }

    @Test
    public void getPatientSet_validCategoricalFilter_returnPatients() throws ExecutionException {
        String conceptPath = "\\open_access-1000Genomes\\data\\POPULATION NAME\\";
        Query query = new Query(
            List.of(), List.of(), new PhenotypicFilter(PhenotypicFilterType.FILTER, conceptPath, Set.of("Finnish"), null, null, null), null,
            ResultType.COUNT, null, null
        );

        Set<Integer> patientIds = Set.of(2, 3, 5, 8, 13);
        when(phenotypicObservationStore.getKeysForValues(conceptPath, Set.of("Finnish"))).thenReturn(patientIds);

        Set<Integer> patientSet = phenotypicQueryExecutor.getPatientSet(query);
        assertEquals(patientIds, patientSet);
    }

    @Test
    public void getPatientSet_nonExistentCategoricalFilter_returnNoPatients() {
        String conceptPath = "\\open_access-1000Genomes\\data\\NOT_A_CONCEPT_PATH\\";
        Query query = new Query(
            List.of(), List.of(), new PhenotypicFilter(PhenotypicFilterType.FILTER, conceptPath, Set.of("Finnish"), null, null, null), null,
            ResultType.COUNT, null, null
        );

        when(phenotypicObservationStore.getKeysForValues(conceptPath, Set.of("Finnish"))).thenReturn(Set.of());

        Set<Integer> patientSet = phenotypicQueryExecutor.getPatientSet(query);
        assertEquals(Set.of(), patientSet);
    }

    @Test
    public void getPatientSet_nonExistentNumericFilter_returnNoPatients() {
        String conceptPath = "\\open_access-1000Genomes\\data\\NOT_A_CONCEPT_PATH\\";
        Query query = new Query(
            List.of(), List.of(), new PhenotypicFilter(PhenotypicFilterType.FILTER, conceptPath, null, 42.0, null, null), null,
            ResultType.COUNT, null, null
        );

        when(phenotypicObservationStore.getKeysForRange(conceptPath, 42.0, null)).thenReturn(Set.of());

        Set<Integer> patientSet = phenotypicQueryExecutor.getPatientSet(query);
        assertEquals(Set.of(), patientSet);
    }


    @Test
    public void getPatientSet_complexNestedFilters_returnPatients() throws ExecutionException {
        String categoricalConcept1 = "\\open_access-1000Genomes\\data\\POPULATION NAME\\";
        String categoricalConcept2 = "\\open_access-1000Genomes\\data\\SEX\\";
        String numericConcept1 = "\\open_access-1000Genomes\\data\\SYNTHETIC_AGE\\";
        String numericConcept2 = "\\open_access-1000Genomes\\data\\SYNTHETIC_HEIGHT\\";

        PhenotypicFilter categoricalFilter1 =
            new PhenotypicFilter(PhenotypicFilterType.FILTER, categoricalConcept1, Set.of("Finnish"), null, null, null);
        PhenotypicFilter numericFilter1 = new PhenotypicFilter(PhenotypicFilterType.FILTER, numericConcept1, null, 42.0, null, null);
        PhenotypicFilter categoricalFilter2 =
            new PhenotypicFilter(PhenotypicFilterType.FILTER, categoricalConcept2, Set.of("female"), null, null, null);
        PhenotypicFilter numericFilter2 = new PhenotypicFilter(PhenotypicFilterType.FILTER, numericConcept2, null, null, 175.5, null);
        PhenotypicClause phenotypicSubquery1 = new PhenotypicSubquery(null, List.of(categoricalFilter1, numericFilter1), Operator.AND);
        PhenotypicClause phenotypicSubquery2 = new PhenotypicSubquery(null, List.of(categoricalFilter2, numericFilter2), Operator.AND);
        PhenotypicClause topSubquery = new PhenotypicSubquery(null, List.of(phenotypicSubquery1, phenotypicSubquery2), Operator.OR);

        Query query = new Query(List.of(), List.of(), topSubquery, List.of(), ResultType.COUNT, null, null);

        Set<Integer> catFilter1Ids = Set.of(3, 5, 8, 13, 21);
        Set<Integer> numFilter1Ids = Set.of(2, 3, 5, 8, 13);
        Set<Integer> catFilter2Ids = Set.of(10, 100, 1000);
        Set<Integer> numFilter2Ids = Set.of(999, 1000, 10001);
        // (catFilter1Ids AND numFilter1Ids) OR (catFilter2Ids AND numFilter2Ids)
        Set<Integer> expectedPatients = Set.of(3, 5, 8, 13, 1000);

        when(phenotypicObservationStore.getKeysForValues(categoricalConcept1, Set.of("Finnish"))).thenReturn(catFilter1Ids);
        when(phenotypicObservationStore.getKeysForValues(categoricalConcept2, Set.of("female"))).thenReturn(catFilter2Ids);
        when(phenotypicObservationStore.getKeysForRange(numericConcept1, 42.0, null)).thenReturn(numFilter1Ids);
        when(phenotypicObservationStore.getKeysForRange(numericConcept2, null, 175.5)).thenReturn(numFilter2Ids);

        Set<Integer> patientSet = phenotypicQueryExecutor.getPatientSet(query);
        assertEquals(expectedPatients, patientSet);
    }

    @Test
    public void getPatientSet_validCategoricalFilterMultipleValues_returnPatients() throws ExecutionException {
        String conceptPath = "\\open_access-1000Genomes\\data\\POPULATION NAME\\";
        Query query = new Query(
            List.of(), List.of(),
            new PhenotypicFilter(PhenotypicFilterType.FILTER, conceptPath, Set.of("Finnish", "Zapotec"), null, null, null), null,
            ResultType.COUNT, null, null
        );

        Set<Integer> patientIds = Set.of(8, 13, 21);
        when(phenotypicObservationStore.getKeysForValues(conceptPath, Set.of("Finnish", "Zapotec"))).thenReturn(patientIds);

        Set<Integer> patientSet = phenotypicQueryExecutor.getPatientSet(query);
        assertEquals(patientIds, patientSet);
    }

    @Test
    public void getPatientSet_validAnyRecordOfFilter_returnPatients() throws ExecutionException {
        String categoricalConceptPath = "\\open_access-1000Genomes\\data\\POPULATION NAME\\";
        String numericConceptPath = "\\open_access-1000Genomes\\data\\SYNTHETIC_AGE\\";
        String nonMatchingConceptPath = "\\synthea\\data\\SYNTHETIC_AGE\\";
        Query query = new Query(
            List.of(), List.of(),
            new PhenotypicFilter(PhenotypicFilterType.ANY_RECORD_OF, "\\open_access-1000Genomes\\", null, null, null, null), null,
            ResultType.COUNT, null, null
        );

        when(phenotypeMetaStore.getChildConceptPaths("\\open_access-1000Genomes\\"))
            .thenReturn(Set.of(categoricalConceptPath, numericConceptPath));
        List<Integer> numericPatientIds = List.of(2, 3, 5);
        List<Integer> categoricalPatientIds = List.of(10, 100, 1000, 100000);

        when(phenotypicObservationStore.getAllKeys(categoricalConceptPath)).thenReturn(categoricalPatientIds);
        when(phenotypicObservationStore.getAllKeys(numericConceptPath)).thenReturn(numericPatientIds);

        Set<Integer> patientSet = phenotypicQueryExecutor.getPatientSet(query);
        Set<Integer> expectedPatients = new HashSet<>();
        expectedPatients.addAll(categoricalPatientIds);
        expectedPatients.addAll(numericPatientIds);
        assertEquals(expectedPatients, patientSet);

        verify(phenotypicObservationStore, times(0)).getAllKeys(nonMatchingConceptPath);
    }

    @Test
    public void getPatientSet_anyRecordOfFilterNoMatches_returnNoPatients() {
        String nonMatchingConceptPath = "\\synthea\\data\\SYNTHETIC_AGE\\";
        Query query = new Query(
            List.of(), List.of(),
            new PhenotypicFilter(PhenotypicFilterType.ANY_RECORD_OF, "\\open_access-1000Genomes\\", null, null, null, null), null,
            ResultType.COUNT, null, null
        );

        when(phenotypeMetaStore.getChildConceptPaths("\\open_access-1000Genomes\\")).thenReturn(Set.of());

        Set<Integer> patientSet = phenotypicQueryExecutor.getPatientSet(query);
        assertEquals(Set.of(), patientSet);
    }


    @Test
    public void getPatientSet_validRequiredFilter_returnPatients() throws ExecutionException {
        String conceptPath = "\\open_access-1000Genomes\\data\\POPULATION NAME\\";
        Query query = new Query(
            List.of(), List.of(), new PhenotypicFilter(PhenotypicFilterType.REQUIRED, conceptPath, null, null, null, null), null,
            ResultType.COUNT, null, null
        );

        List<Integer> keyList = List.of(2, 3, 5, 8, 13, 13, 8, 5);
        when(phenotypicObservationStore.getAllKeys(conceptPath)).thenReturn(keyList);

        Set<Integer> patientSet = phenotypicQueryExecutor.getPatientSet(query);
        assertEquals(new HashSet<>(keyList), patientSet);
    }

    @Test
    public void getPatientSet_notFoundRequiredFilter_returnNoPatients() {
        String conceptPath = "\\open_access-1000Genomes\\data\\POPULATION NAME\\";
        Query query = new Query(
            List.of(), List.of(), new PhenotypicFilter(PhenotypicFilterType.REQUIRED, conceptPath, null, null, null, null), null,
            ResultType.COUNT, null, null
        );

        when(phenotypicObservationStore.getAllKeys(conceptPath)).thenReturn(List.of());

        Set<Integer> patientSet = phenotypicQueryExecutor.getPatientSet(query);
        assertEquals(Set.of(), patientSet);
    }

    /**
     * Regression test for a cross-study consent leak.
     *
     * Setup mirrors the production bug:
     *   - User has phs001001.c1, phs001062.c1, phs001062.c2 (NOT phs001001.c2).
     *   - Patient 1 is in phs001001.c2 AND phs001062.c2 (overlapping).
     *   - Patient 2 is in phs001001.c2 only.
     *   - Query filters on a phs001001 variable whose value only appears for phs001001.c2 patients.
     *
     * With the global-OR authorization filter, Patient 1 leaks through because their
     * phs001062.c2 membership satisfies the auth filter. Expected behavior: neither patient
     * should be returned because the user is not authorized for phs001001.c2.
     */
    @Test
    public void getPatientSet_overlappingConsentsAcrossStudies_doesNotLeakPatients() {
        String phs001001DiseasePath = "\\phs001001\\data\\disease\\";
        String consentsPath = "\\_consents\\";

        Map<String, Set<Integer>> consentMembership = Map.of(
            "phs001001.c1", Set.of(),
            "phs001001.c2", Set.of(1, 2),
            "phs001062.c1", Set.of(),
            "phs001062.c2", Set.of(1)
        );
        stubConsentsCube(consentsPath, consentMembership);
        // Both patients 1 and 2 have the disease value (disease filter alone doesn't distinguish them).
        when(phenotypicObservationStore.getKeysForValues(eq(phs001001DiseasePath), eq(Set.of("DS-AF-IRB-RD"))))
            .thenReturn(Set.of(1, 2));

        PhenotypicFilter diseaseFilter =
            new PhenotypicFilter(PhenotypicFilterType.FILTER, phs001001DiseasePath, Set.of("DS-AF-IRB-RD"), null, null, null);
        AuthorizationFilter authFilter = new AuthorizationFilter(consentsPath, Set.of("phs001001.c1", "phs001062.c1", "phs001062.c2"));
        Query query = new Query(
            List.of(), List.of(authFilter), diseaseFilter, null, ResultType.COUNT, null, null
        );

        Set<Integer> patientSet = phenotypicQueryExecutor.getPatientSet(query);
        assertEquals(Set.of(), patientSet);
    }

    /**
     * Positive counterpart to the overlap regression: a patient who *is* in the user's allowed
     * phs001001 consent should still come through, and the per-study auth filter must not
     * require membership in studies the query doesn't reference.
     */
    @Test
    public void getPatientSet_authorizedPatientInReferencedStudy_returnsPatient() {
        String phs001001DiseasePath = "\\phs001001\\data\\disease\\";
        String consentsPath = "\\_consents\\";

        Map<String, Set<Integer>> consentMembership = Map.of(
            "phs001001.c1", Set.of(42),
            "phs001001.c2", Set.of(99),
            "phs001062.c1", Set.of(),
            "phs001062.c2", Set.of()
        );
        stubConsentsCube(consentsPath, consentMembership);
        // Patient 42 (authorized via c1) and patient 99 (unauthorized c2-only) both have the disease value.
        when(phenotypicObservationStore.getKeysForValues(eq(phs001001DiseasePath), eq(Set.of("DS-AF-IRB-RD"))))
            .thenReturn(Set.of(42, 99));

        PhenotypicFilter diseaseFilter =
            new PhenotypicFilter(PhenotypicFilterType.FILTER, phs001001DiseasePath, Set.of("DS-AF-IRB-RD"), null, null, null);
        AuthorizationFilter authFilter = new AuthorizationFilter(consentsPath, Set.of("phs001001.c1", "phs001062.c1", "phs001062.c2"));
        Query query = new Query(
            List.of(), List.of(authFilter), diseaseFilter, null, ResultType.COUNT, null, null
        );

        Set<Integer> patientSet = phenotypicQueryExecutor.getPatientSet(query);
        assertEquals(Set.of(42), patientSet);
    }

    /**
     * Stubs the _consents cube so that getKeysForValues returns the union of patients
     * for whichever consent values the executor asks about. This mirrors the real
     * multi-value PhenoCube semantics: a patient can belong to multiple consent values,
     * and any requested value-set yields the union of member patients.
     */
    private void stubConsentsCube(String consentsPath, Map<String, Set<Integer>> consentMembership) {
        lenient().when(phenotypicObservationStore.getKeysForValues(eq(consentsPath), any())).thenAnswer(invocation -> {
            Collection<String> requested = invocation.getArgument(1);
            Set<Integer> union = new HashSet<>();
            for (String value : requested) {
                union.addAll(consentMembership.getOrDefault(value, Set.of()));
            }
            return union;
        });
    }
}

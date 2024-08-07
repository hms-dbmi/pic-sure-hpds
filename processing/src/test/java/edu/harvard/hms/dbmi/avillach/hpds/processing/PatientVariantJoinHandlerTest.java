package edu.harvard.hms.dbmi.avillach.hpds.processing;

import edu.harvard.hms.dbmi.avillach.hpds.data.genotype.VariantMasks;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.math.BigInteger;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class PatientVariantJoinHandlerTest {

    @Mock
    private VariantService variantService;

    private PatientVariantJoinHandler patientVariantJoinHandler;

    public static final String[] PATIENT_IDS = {"101", "102", "103", "104", "105", "106", "107", "108"};
    public static final Set<Integer> PATIENT_IDS_INTEGERS = Set.of(PATIENT_IDS).stream().map(Integer::parseInt).collect(Collectors.toSet());
    public static final String[] VARIANT_INDEX = {"16,61642243,A,T", "16,61642252,A,G", "16,61642256,C,T", "16,61642257,G,A", "16,61642258,G,A", "16,61642259,G,A", "16,61642260,G,A", "16,61642261,G,A"};

    @Before
    public void setUp() {
        patientVariantJoinHandler = new PatientVariantJoinHandler(variantService);
        when(variantService.getVariantIndex()).thenReturn(VARIANT_INDEX);
    }

    @Test
    public void getPatientIdsForIntersectionOfVariantSets_allPatientsMatchOneVariant() {
        VariantIndex intersectionOfInfoFilters = new SparseVariantIndex(Set.of(0, 2, 4));
        when(variantService.getPatientIds()).thenReturn(PATIENT_IDS);
        when(variantService.emptyBitmask()).thenReturn(emptyBitmask(PATIENT_IDS));

        BigInteger maskForAllPatients = patientVariantJoinHandler.createMaskForPatientSet(PATIENT_IDS_INTEGERS);
        BigInteger maskForNoPatients = patientVariantJoinHandler.createMaskForPatientSet(Set.of());

        VariantMasks variantMasks = new VariantMasks(new String[0]);
        variantMasks.heterozygousMask = maskForAllPatients;
        VariantMasks emptyVariantMasks = new VariantMasks(new String[0]);
        emptyVariantMasks.heterozygousMask = maskForNoPatients;
        when(variantService.getMasks(eq(VARIANT_INDEX[0]), any())).thenReturn(variantMasks);
        when(variantService.getMasks(eq(VARIANT_INDEX[2]), any())).thenReturn(emptyVariantMasks);
        when(variantService.getMasks(eq(VARIANT_INDEX[4]), any())).thenReturn(emptyVariantMasks);

        List<Set<Integer>> patientIdsForIntersectionOfVariantSets = patientVariantJoinHandler.getPatientIdsForIntersectionOfVariantSets(List.of(), intersectionOfInfoFilters);
        // this should be all patients, as all patients match one of the variants
        assertEquals(PATIENT_IDS_INTEGERS, patientIdsForIntersectionOfVariantSets.get(0));
    }

    @Test
    public void getPatientIdsForIntersectionOfVariantSets_noPatientsMatchVariants() {
        VariantIndex intersectionOfInfoFilters = new SparseVariantIndex(Set.of(0, 2, 4));
        when(variantService.getPatientIds()).thenReturn(PATIENT_IDS);
        when(variantService.emptyBitmask()).thenReturn(emptyBitmask(PATIENT_IDS));

        BigInteger maskForNoPatients = patientVariantJoinHandler.createMaskForPatientSet(Set.of());
        VariantMasks emptyVariantMasks = new VariantMasks(new String[0]);
        emptyVariantMasks.heterozygousMask = maskForNoPatients;
        when(variantService.getMasks(eq(VARIANT_INDEX[0]), any())).thenReturn(emptyVariantMasks);
        when(variantService.getMasks(eq(VARIANT_INDEX[2]), any())).thenReturn(emptyVariantMasks);
        when(variantService.getMasks(eq(VARIANT_INDEX[4]), any())).thenReturn(emptyVariantMasks);

        List<Set<Integer>> patientIdsForIntersectionOfVariantSets = patientVariantJoinHandler.getPatientIdsForIntersectionOfVariantSets(List.of(), intersectionOfInfoFilters);
        // this should be empty because all variants masks have no matching patients
        assertEquals(Set.of(), patientIdsForIntersectionOfVariantSets.get(0));
    }

    @Test
    public void getPatientIdsForIntersectionOfVariantSets_somePatientsMatchVariants() {
        VariantIndex intersectionOfInfoFilters = new SparseVariantIndex(Set.of(0, 2, 4));
        when(variantService.getPatientIds()).thenReturn(PATIENT_IDS);
        when(variantService.emptyBitmask()).thenReturn(emptyBitmask(PATIENT_IDS));


        BigInteger maskForPatients1 = patientVariantJoinHandler.createMaskForPatientSet(Set.of(101, 103));
        BigInteger maskForPatients2 = patientVariantJoinHandler.createMaskForPatientSet(Set.of(103, 105));
        VariantMasks variantMasks = new VariantMasks(new String[0]);
        variantMasks.heterozygousMask = maskForPatients1;
        VariantMasks variantMasks2 = new VariantMasks(new String[0]);
        variantMasks2.heterozygousMask = maskForPatients2;
        when(variantService.getMasks(eq(VARIANT_INDEX[0]), any())).thenReturn(variantMasks);
        when(variantService.getMasks(eq(VARIANT_INDEX[2]), any())).thenReturn(variantMasks2);

        List<Set<Integer>> patientIdsForIntersectionOfVariantSets = patientVariantJoinHandler.getPatientIdsForIntersectionOfVariantSets(List.of(), intersectionOfInfoFilters);
        // this should be all patients who match at least one variant
        assertEquals(Set.of(101, 103, 105), patientIdsForIntersectionOfVariantSets.get(0));
    }

    @Test
    public void getPatientIdsForIntersectionOfVariantSets_noVariants() {
        VariantIndex intersectionOfInfoFilters = new SparseVariantIndex(Set.of());

        List<Set<Integer>> patientIdsForIntersectionOfVariantSets = patientVariantJoinHandler.getPatientIdsForIntersectionOfVariantSets(List.of(), intersectionOfInfoFilters);
        // this should be empty, as there are no variants
        assertEquals(Set.of(), patientIdsForIntersectionOfVariantSets.get(0));
    }

    @Test
    public void getPatientIdsForIntersectionOfVariantSets_patientSubsetPassed() {
        VariantIndex intersectionOfInfoFilters = new SparseVariantIndex(Set.of(0, 2, 4));
        when(variantService.getPatientIds()).thenReturn(PATIENT_IDS);
        when(variantService.emptyBitmask()).thenReturn(emptyBitmask(PATIENT_IDS));

        BigInteger maskForPatients1 = patientVariantJoinHandler.createMaskForPatientSet(Set.of(101, 103, 105));
        BigInteger maskForPatients2 = patientVariantJoinHandler.createMaskForPatientSet(Set.of(103, 105, 107));
        VariantMasks variantMasks = new VariantMasks(new String[0]);
        variantMasks.heterozygousMask = maskForPatients1;
        VariantMasks variantMasks2 = new VariantMasks(new String[0]);
        variantMasks2.heterozygousMask = maskForPatients2;
        when(variantService.getMasks(eq(VARIANT_INDEX[0]), any())).thenReturn(variantMasks);
        when(variantService.getMasks(eq(VARIANT_INDEX[2]), any())).thenReturn(variantMasks2);

        List<Set<Integer>> patientIdsForIntersectionOfVariantSets = patientVariantJoinHandler.getPatientIdsForIntersectionOfVariantSets(List.of(Set.of(102, 103, 104, 105, 106)), intersectionOfInfoFilters);
        // this should be the union of patients matching variants (101, 103, 105, 107), intersected with the patient subset parameter (103, 104, 105) which is (103, 105)
        assertEquals(Set.of(103, 105), patientIdsForIntersectionOfVariantSets.get(1));
    }

    public BigInteger emptyBitmask(String[] patientIds) {
        String emptyVariantMask = "";
        for (String patientId : patientIds) {
            emptyVariantMask = emptyVariantMask + "0";
        }
        return new BigInteger("11" + emptyVariantMask + "11", 2);
    }
}
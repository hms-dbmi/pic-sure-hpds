package edu.harvard.hms.dbmi.avillach.hpds.processing;


import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import edu.harvard.hms.dbmi.avillach.hpds.data.genotype.FileBackedByteIndexedInfoStore;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.PhenoCube;
import edu.harvard.hms.dbmi.avillach.hpds.data.query.Query;
import edu.harvard.hms.dbmi.avillach.hpds.storage.FileBackedByteIndexedStorage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.mockito.ArgumentMatchers.any;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class AbstractProcessorTest {

    private AbstractProcessor abstractProcessor;

    private Map<String, FileBackedByteIndexedInfoStore> infoStores;

    @Mock
    private VariantService variantService;

    @Mock
    private VariantIndexCache variantIndexCache;

    @Mock
    private PatientVariantJoinHandler patientVariantJoinHandler;

    @Mock
    private LoadingCache<String, PhenoCube<?>> mockLoadingCache;

    public static final String GENE_WITH_VARIANT_KEY = "Gene_with_variant";
    private static final String VARIANT_SEVERITY_KEY = "Variant_severity";
    public static final List<String> EXAMPLE_GENES_WITH_VARIANT = List.of("CDH8", "CDH9", "CDH10");
    public static final List<String> EXAMPLE_VARIANT_SEVERITIES = List.of("HIGH", "MODERATE", "LOW");


    @Before
    public void setup() {
        FileBackedByteIndexedInfoStore mockInfoStore = mock(FileBackedByteIndexedInfoStore.class);
        FileBackedByteIndexedStorage<String, String[]> mockIndexedStorage = mock(FileBackedByteIndexedStorage.class);
        when(mockIndexedStorage.keys()).thenReturn(new HashSet<>(EXAMPLE_GENES_WITH_VARIANT));
        when(mockInfoStore.getAllValues()).thenReturn(mockIndexedStorage);

        FileBackedByteIndexedInfoStore mockInfoStore2 = mock(FileBackedByteIndexedInfoStore.class);
        FileBackedByteIndexedStorage<String, String[]> mockIndexedStorage2 = mock(FileBackedByteIndexedStorage.class);
        when(mockIndexedStorage2.keys()).thenReturn(new HashSet<>(EXAMPLE_VARIANT_SEVERITIES));
        when(mockInfoStore2.getAllValues()).thenReturn(mockIndexedStorage2);

        infoStores = Map.of(
                GENE_WITH_VARIANT_KEY, mockInfoStore,
                VARIANT_SEVERITY_KEY, mockInfoStore2
        );

        abstractProcessor = new AbstractProcessor(
                new PhenotypeMetaStore(
                        new TreeMap<>(),
                        new TreeSet<>()
                ),
                mockLoadingCache,
                infoStores,
                null,
                variantService,
                variantIndexCache,
                patientVariantJoinHandler
        );
    }

    @Test
    public void getPatientSubsetForQuery_oneVariantCategoryFilter_indexFound() {
        when(variantIndexCache.get(GENE_WITH_VARIANT_KEY, EXAMPLE_GENES_WITH_VARIANT.get(0))).thenReturn(new SparseVariantIndex(Set.of(2, 4, 6)));

        ArgumentCaptor<VariantIndex> argumentCaptor = ArgumentCaptor.forClass(VariantIndex.class);
        when(patientVariantJoinHandler.getPatientIdsForIntersectionOfVariantSets(any(), argumentCaptor.capture())).thenReturn(List.of(Set.of(42)));

        Map<String, String[]> categoryVariantInfoFilters =
                Map.of(GENE_WITH_VARIANT_KEY, new String[] {EXAMPLE_GENES_WITH_VARIANT.get(0)});
        Query.VariantInfoFilter variantInfoFilter = new Query.VariantInfoFilter();
        variantInfoFilter.categoryVariantInfoFilters = categoryVariantInfoFilters;

        List<Query.VariantInfoFilter> variantInfoFilters = List.of(variantInfoFilter);

        Query query = new Query();
        query.setVariantInfoFilters(variantInfoFilters);

        TreeSet<Integer> patientSubsetForQuery = abstractProcessor.getPatientSubsetForQuery(query);
        assertFalse(patientSubsetForQuery.isEmpty());
        assertEquals(argumentCaptor.getValue(), new SparseVariantIndex(Set.of(2,4,6)));
    }

    @Test
    public void getPatientSubsetForQuery_oneVariantCategoryFilterTwoValues_unionFilters() {
        when(variantIndexCache.get(GENE_WITH_VARIANT_KEY, EXAMPLE_GENES_WITH_VARIANT.get(0))).thenReturn(new SparseVariantIndex(Set.of(2, 4)));
        when(variantIndexCache.get(GENE_WITH_VARIANT_KEY, EXAMPLE_GENES_WITH_VARIANT.get(1))).thenReturn(new SparseVariantIndex(Set.of(6)));

        ArgumentCaptor<VariantIndex> argumentCaptor = ArgumentCaptor.forClass(VariantIndex.class);
        when(patientVariantJoinHandler.getPatientIdsForIntersectionOfVariantSets(any(), argumentCaptor.capture())).thenReturn(List.of(Set.of(42)));

        Map<String, String[]> categoryVariantInfoFilters =
                Map.of(GENE_WITH_VARIANT_KEY, new String[] {EXAMPLE_GENES_WITH_VARIANT.get(0), EXAMPLE_GENES_WITH_VARIANT.get(1)});
        Query.VariantInfoFilter variantInfoFilter = new Query.VariantInfoFilter();
        variantInfoFilter.categoryVariantInfoFilters = categoryVariantInfoFilters;

        List<Query.VariantInfoFilter> variantInfoFilters = List.of(variantInfoFilter);

        Query query = new Query();
        query.setVariantInfoFilters(variantInfoFilters);

        TreeSet<Integer> patientSubsetForQuery = abstractProcessor.getPatientSubsetForQuery(query);
        assertFalse(patientSubsetForQuery.isEmpty());
        // Expected result is the union of the two values
        assertEquals(argumentCaptor.getValue(), new SparseVariantIndex(Set.of(2,4,6)));
    }

    @Test
    public void getPatientSubsetForQuery_twoVariantCategoryFilters_intersectFilters() {
        when(variantIndexCache.get(GENE_WITH_VARIANT_KEY, EXAMPLE_GENES_WITH_VARIANT.get(0))).thenReturn(new SparseVariantIndex(Set.of(2, 4, 6)));
        when(variantIndexCache.get(VARIANT_SEVERITY_KEY, EXAMPLE_VARIANT_SEVERITIES.get(0))).thenReturn(new SparseVariantIndex(Set.of(4, 5, 6, 7)));

        ArgumentCaptor<VariantIndex> argumentCaptor = ArgumentCaptor.forClass(VariantIndex.class);
        when(patientVariantJoinHandler.getPatientIdsForIntersectionOfVariantSets(any(), argumentCaptor.capture())).thenReturn(List.of(Set.of(42)));

        Map<String, String[]> categoryVariantInfoFilters = Map.of(
                GENE_WITH_VARIANT_KEY, new String[] {EXAMPLE_GENES_WITH_VARIANT.get(0)},
                VARIANT_SEVERITY_KEY, new String[] {EXAMPLE_VARIANT_SEVERITIES.get(0)}
        );
        Query.VariantInfoFilter variantInfoFilter = new Query.VariantInfoFilter();
        variantInfoFilter.categoryVariantInfoFilters = categoryVariantInfoFilters;

        List<Query.VariantInfoFilter> variantInfoFilters = List.of(variantInfoFilter);

        Query query = new Query();
        query.setVariantInfoFilters(variantInfoFilters);

        TreeSet<Integer> patientSubsetForQuery = abstractProcessor.getPatientSubsetForQuery(query);
        assertFalse(patientSubsetForQuery.isEmpty());
        // Expected result is the intersection of the two filters
        assertEquals(argumentCaptor.getValue(), new SparseVariantIndex(Set.of(4, 6)));
    }

    @Test
    public void getPatientSubsetForQuery_anyRecordOf_applyOrLogic() throws ExecutionException {
        when(variantIndexCache.get(GENE_WITH_VARIANT_KEY, EXAMPLE_GENES_WITH_VARIANT.get(0))).thenReturn(new SparseVariantIndex(Set.of(2, 4, 6)));
        when(variantIndexCache.get(VARIANT_SEVERITY_KEY, EXAMPLE_VARIANT_SEVERITIES.get(0))).thenReturn(new SparseVariantIndex(Set.of(4, 5, 6, 7)));

        ArgumentCaptor<VariantIndex> argumentCaptor = ArgumentCaptor.forClass(VariantIndex.class);
        ArgumentCaptor<List<Set<Integer>>> listArgumentCaptor = ArgumentCaptor.forClass(List.class);
        when(patientVariantJoinHandler.getPatientIdsForIntersectionOfVariantSets(listArgumentCaptor.capture(), argumentCaptor.capture())).thenReturn(List.of(Set.of(42)));

        Map<String, String[]> categoryVariantInfoFilters = Map.of(
                GENE_WITH_VARIANT_KEY, new String[] {EXAMPLE_GENES_WITH_VARIANT.get(0)},
                VARIANT_SEVERITY_KEY, new String[] {EXAMPLE_VARIANT_SEVERITIES.get(0)}
        );
        Query.VariantInfoFilter variantInfoFilter = new Query.VariantInfoFilter();
        variantInfoFilter.categoryVariantInfoFilters = categoryVariantInfoFilters;

        List<Query.VariantInfoFilter> variantInfoFilters = List.of(variantInfoFilter);

        PhenoCube mockPhenoCube = mock(PhenoCube.class);
        when(mockPhenoCube.keyBasedIndex()).thenReturn(List.of(42, 101));
        when(mockLoadingCache.get("good concept")).thenReturn(mockPhenoCube);
        when(mockLoadingCache.get("bad concept")).thenThrow(CacheLoader.InvalidCacheLoadException.class);

        Query query = new Query();
        query.setVariantInfoFilters(variantInfoFilters);
        query.setAnyRecordOf(List.of("good concept", "bad concept"));

        TreeSet<Integer> patientSubsetForQuery = abstractProcessor.getPatientSubsetForQuery(query);
        assertFalse(patientSubsetForQuery.isEmpty());
        // Expected result is the intersection of the two filters
        assertEquals(argumentCaptor.getValue(), new SparseVariantIndex(Set.of(4, 6)));
        assertEquals(listArgumentCaptor.getValue().get(0), Set.of(42, 101));
    }



    @Test
    public void getPatientSubsetForQuery_anyRecordOfInvalidKey_returnEmpty() throws ExecutionException {
        when(variantIndexCache.get(GENE_WITH_VARIANT_KEY, EXAMPLE_GENES_WITH_VARIANT.get(0))).thenReturn(new SparseVariantIndex(Set.of(2, 4, 6)));
        when(variantIndexCache.get(VARIANT_SEVERITY_KEY, EXAMPLE_VARIANT_SEVERITIES.get(0))).thenReturn(new SparseVariantIndex(Set.of(4, 5, 6, 7)));

        ArgumentCaptor<VariantIndex> argumentCaptor = ArgumentCaptor.forClass(VariantIndex.class);
        ArgumentCaptor<List<Set<Integer>>> listArgumentCaptor = ArgumentCaptor.forClass(List.class);
        when(patientVariantJoinHandler.getPatientIdsForIntersectionOfVariantSets(listArgumentCaptor.capture(), argumentCaptor.capture())).thenReturn(List.of(Set.of(42)));

        Map<String, String[]> categoryVariantInfoFilters = Map.of(
                GENE_WITH_VARIANT_KEY, new String[] {EXAMPLE_GENES_WITH_VARIANT.get(0)},
                VARIANT_SEVERITY_KEY, new String[] {EXAMPLE_VARIANT_SEVERITIES.get(0)}
        );
        Query.VariantInfoFilter variantInfoFilter = new Query.VariantInfoFilter();
        variantInfoFilter.categoryVariantInfoFilters = categoryVariantInfoFilters;

        List<Query.VariantInfoFilter> variantInfoFilters = List.of(variantInfoFilter);

        when(mockLoadingCache.get("bad concept")).thenThrow(CacheLoader.InvalidCacheLoadException.class);

        Query query = new Query();
        query.setVariantInfoFilters(variantInfoFilters);
        query.setAnyRecordOf(List.of("bad concept"));

        TreeSet<Integer> patientSubsetForQuery = abstractProcessor.getPatientSubsetForQuery(query);
        assertFalse(patientSubsetForQuery.isEmpty());
        // Expected result is the intersection of the two filters
        assertEquals(argumentCaptor.getValue(), new SparseVariantIndex(Set.of(4, 6)));
        assertEquals(listArgumentCaptor.getValue().get(0), Set.of());
    }
}

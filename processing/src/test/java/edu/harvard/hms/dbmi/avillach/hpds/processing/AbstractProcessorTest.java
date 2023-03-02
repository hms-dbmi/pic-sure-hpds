package edu.harvard.hms.dbmi.avillach.hpds.processing;


import com.google.common.cache.LoadingCache;
import edu.harvard.hms.dbmi.avillach.hpds.data.genotype.FileBackedByteIndexedInfoStore;
import edu.harvard.hms.dbmi.avillach.hpds.data.genotype.VariantStore;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.ColumnMeta;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.PhenoCube;
import edu.harvard.hms.dbmi.avillach.hpds.data.query.Query;
import edu.harvard.hms.dbmi.avillach.hpds.storage.FileBackedByteIndexedStorage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class AbstractProcessorTest {

    private AbstractProcessor abstractProcessor;

    private TreeMap<String, ColumnMeta> metaStore;

    private TreeSet<Integer> allIds;
    private LoadingCache<String, PhenoCube<?>> store;
    private List<String> infoStoreColumns;

    private Map<String, FileBackedByteIndexedInfoStore> infoStores;

    @Mock
    private VariantStore variantStore;


    @Before
    public void setup() throws IOException {
        PhenotypeMetaStore phenotypeMetaStore = new PhenotypeMetaStore(
                new TreeMap<>(),
                new TreeSet<>()
        );
        FileBackedByteIndexedInfoStore mockInfoStore = mock(FileBackedByteIndexedInfoStore.class);
        FileBackedByteIndexedStorage<String, String[]> mockIndexedStorage = mock(FileBackedByteIndexedStorage.class);
        when(mockIndexedStorage.get("test1")).thenReturn(new String[] {"variant1"});
        when(mockIndexedStorage.keys()).thenReturn(Set.of("test1"));
        when(mockInfoStore.getAllValues()).thenReturn(mockIndexedStorage);
        infoStores = Map.of("FILTERKEY", mockInfoStore);
        abstractProcessor = new AbstractProcessor(
                phenotypeMetaStore,
                store,
                infoStores,
                infoStoreColumns,
                variantStore
        );
    }

    @Test
    public void testVariantListWithVariantInfoFiltersWithMultipleVariantsButNoIntersectionKeys() throws Exception {
        String[] patientIds = new String[] {"42", "99"};
        when(variantStore.getPatientIds()).thenReturn(patientIds);

        Map<String, String[]> categoryVariantInfoFilters =
                Map.of("FILTERKEY", new String[] {"test1"});
        Query.VariantInfoFilter variantInfoFilter = new Query.VariantInfoFilter();
        variantInfoFilter.categoryVariantInfoFilters = categoryVariantInfoFilters;

        List<Query.VariantInfoFilter> variantInfoFilters = List.of(variantInfoFilter);

        Query query = new Query();
        query.setVariantInfoFilters(variantInfoFilters);

        Collection<String> variantList = abstractProcessor.getVariantList(query);
    }
}

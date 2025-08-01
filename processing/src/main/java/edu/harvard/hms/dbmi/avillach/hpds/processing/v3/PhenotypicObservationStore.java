package edu.harvard.hms.dbmi.avillach.hpds.processing.v3;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import edu.harvard.hms.dbmi.avillach.hpds.crypto.Crypto;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.ColumnMeta;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.PhenoCube;
import edu.harvard.hms.dbmi.avillach.hpds.processing.PhenotypeMetaStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutionException;

@Component
public class PhenotypicObservationStore {

    private static Logger log = LoggerFactory.getLogger(PhenotypicObservationStore.class);
    private final int CACHE_SIZE;

    private final LoadingCache<String, PhenoCube<?>> phenoCubeCache;

    private final String hpdsDataDirectory;

    private final PhenotypeMetaStore phenotypeMetaStore;

    @Autowired
    public PhenotypicObservationStore(
        PhenotypeMetaStore phenotypeMetaStore, @Value("${HPDS_DATA_DIRECTORY:/opt/local/hpds/}") String hpdsDataDirectory
    ) {
        this.phenotypeMetaStore = phenotypeMetaStore;
        this.hpdsDataDirectory = hpdsDataDirectory;
        CACHE_SIZE = Integer.parseInt(System.getProperty("CACHE_SIZE", "100"));
        phenoCubeCache = CacheBuilder.newBuilder().maximumSize(CACHE_SIZE).build(new CacheLoader<>() {
            public PhenoCube<?> load(String key) {
                return loadPhenoCube(key);
            }
        });
    }

    // Constructor for testing only
    public PhenotypicObservationStore(
        PhenotypeMetaStore phenotypeMetaStore, LoadingCache<String, PhenoCube<?>> phenoCubeCache, int cacheSize
    ) {
        this.phenotypeMetaStore = phenotypeMetaStore;
        this.phenoCubeCache = phenoCubeCache;
        this.CACHE_SIZE = cacheSize;
        this.hpdsDataDirectory = "";
    }

    public PhenoCube<?> loadPhenoCube(String key) {
        try (RandomAccessFile allObservationsStore = new RandomAccessFile(hpdsDataDirectory + "allObservationsStore.javabin", "r");) {
            ColumnMeta columnMeta = phenotypeMetaStore.getColumnMeta(key);
            if (columnMeta != null) {
                allObservationsStore.seek(columnMeta.getAllObservationsOffset());
                int length = (int) (columnMeta.getAllObservationsLength() - columnMeta.getAllObservationsOffset());
                byte[] buffer = new byte[length];
                allObservationsStore.read(buffer);
                allObservationsStore.close();
                try (ObjectInputStream inStream = new ObjectInputStream(new ByteArrayInputStream(Crypto.decryptData(buffer)))) {
                    return (PhenoCube<?>) inStream.readObject();
                }
            } else {
                log.warn("ColumnMeta not found for : [{}]", key);
                return null;
            }
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public Set<Integer> getKeysForRange(String conceptPath, Double min, Double max) {
        return getCube(conceptPath).map(cube -> ((PhenoCube<Double>) cube).getKeysForRange(min, max)).orElseGet(Set::of);
    }

    public Set<Integer> getKeysForValues(String conceptPath, Collection<String> values) {
        return getCube(conceptPath).map(cube -> {
            return values.stream().map(value -> ((PhenoCube<String>) cube).getKeysForValue(value)).reduce((set1, set2) -> {
                Set<Integer> union = new HashSet<>(set1);
                union.addAll(set2);
                return union;
            }).orElseGet(Set::of);
        }).orElseGet(Set::of);
    }

    public List<Integer> getAllKeys(String conceptPath) {
        return getCube(conceptPath).map(PhenoCube::keyBasedIndex).orElseGet(List::of);
    }

    /**
     * Get a cube without throwing an error if not found. Useful for federated pic-sure's where there are fewer guarantees about concept
     * paths.
     */
    public Optional<PhenoCube<?>> getCube(String path) {
        try {
            return Optional.ofNullable(phenoCubeCache.get(path));
        } catch (CacheLoader.InvalidCacheLoadException | ExecutionException e) {
            return Optional.empty();
        }
    }

    public Set<String> getCachedKeys() {
        return phenoCubeCache.asMap().keySet();
    }
}

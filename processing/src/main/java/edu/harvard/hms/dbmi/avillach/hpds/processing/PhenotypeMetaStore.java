package edu.harvard.hms.dbmi.avillach.hpds.processing;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.ColumnMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

@Component
public class PhenotypeMetaStore {

    private static final Logger log = LoggerFactory.getLogger(PhenotypeMetaStore.class);


    // Todo: Test using hash map/sets here
    private TreeMap<String, ColumnMeta> metaStore;

    private TreeSet<Integer> patientIds;

    private final Map<String, String> conceptToShardDir;

    private final LoadingCache<String, Set<String>> childConceptCache;

    public TreeMap<String, ColumnMeta> getMetaStore() {
        return metaStore;
    }

    public TreeSet<Integer> getPatientIds() {
        return patientIds;
    }

    public Set<String> getColumnNames() {
        return metaStore.keySet();
    }

    public ColumnMeta getColumnMeta(String columnName) {
        return metaStore.get(columnName);
    }

    public String getShardDirectory(String conceptPath) {
        return conceptToShardDir.get(conceptPath);
    }

    public Set<String> loadChildConceptPaths(String conceptPath) {
        return metaStore.keySet().stream().filter(column -> column.startsWith(conceptPath)).collect(Collectors.toSet());
    }

    public Set<String> getChildConceptPaths(String conceptPath) {
        try {
            return childConceptCache.get(conceptPath);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Autowired
    @SuppressWarnings("unchecked")
    public PhenotypeMetaStore(
        @Value("${HPDS_DATA_DIRECTORY:/opt/local/hpds/}") String hpdsDataDirectory,
        @Value("${CHILD_CONCEPT_CACHE_SIZE:500}") int childConceptCacheSize,
        @Value("${HPDS_SHARDING_ENABLED:false}") boolean shardingEnabled
    ) {
        metaStore = new TreeMap<>();
        patientIds = new TreeSet<>();
        conceptToShardDir = new HashMap<>();

        boolean foundAny = false;

        // 1. Check for root-level columnMeta.javabin
        String rootColumnMetaFile = hpdsDataDirectory + "columnMeta.javabin";
        File rootFile = new File(rootColumnMetaFile);
        if (rootFile.exists()) {
            log.info("Found columnMeta.javabin at root: {}", rootColumnMetaFile);
            loadShardMetadata(rootColumnMetaFile, hpdsDataDirectory);
            foundAny = true;
        }

        // 2. If sharding is enabled, also scan for shard subdirectories
        boolean foundShards = false;
        if (shardingEnabled) {
            File dataDir = new File(hpdsDataDirectory);
            File[] subdirs = dataDir.listFiles(File::isDirectory);
            if (subdirs != null) {
                for (File subdir : subdirs) {
                    File shardColumnMeta = new File(subdir, "columnMeta.javabin");
                    if (shardColumnMeta.exists()) {
                        String shardDir = subdir.getAbsolutePath() + "/";
                        log.info("Found shard columnMeta.javabin in: {}", shardDir);
                        loadShardMetadata(shardColumnMeta.getAbsolutePath(), shardDir);
                        foundShards = true;
                        foundAny = true;
                    }
                }
            }

            if (rootFile.exists() && foundShards) {
                log.warn("Both root columnMeta.javabin and shard subdirectories found. Loading both for migration support.");
            }
        }

        if (!foundAny) {
            log.warn("************************************************");
            log.warn("No columnMeta.javabin found at {}{}.", hpdsDataDirectory, shardingEnabled ? " or in any subdirectory" : "");
            log.warn("If you meant to include phenotype data of any kind, please check that the data directory is correct.");
            log.warn("************************************************");
        }

        childConceptCache = CacheBuilder.newBuilder().maximumSize(childConceptCacheSize).build(new CacheLoader<>() {
            @Override
            public Set<String> load(String key) {
                return loadChildConceptPaths(key);
            }
        });
    }

    @SuppressWarnings("unchecked")
    private void loadShardMetadata(String columnMetaFilePath, String shardDirectory) {
        try (ObjectInputStream objectInputStream = new ObjectInputStream(new GZIPInputStream(new FileInputStream(columnMetaFilePath)))) {
            TreeMap<String, ColumnMeta> shardMeta = (TreeMap<String, ColumnMeta>) objectInputStream.readObject();
            TreeSet<Integer> shardPatientIds = (TreeSet<Integer>) objectInputStream.readObject();

            for (Map.Entry<String, ColumnMeta> entry : shardMeta.entrySet()) {
                String conceptPath = entry.getKey().replaceAll("\\ufffd", "");
                if (metaStore.containsKey(conceptPath)) {
                    log.error("Duplicate concept '{}' found in shard {}. Keeping first occurrence.", conceptPath, shardDirectory);
                } else {
                    metaStore.put(conceptPath, entry.getValue());
                    conceptToShardDir.put(conceptPath, shardDirectory);
                }
            }

            patientIds.addAll(shardPatientIds);
            log.info("Loaded {} concepts and {} patient IDs from {}", shardMeta.size(), shardPatientIds.size(), columnMetaFilePath);
        } catch (IOException | ClassNotFoundException e) {
            log.warn("Could not load metastore from {}", columnMetaFilePath, e);
        }
    }

    public PhenotypeMetaStore(
        TreeMap<String, ColumnMeta> metaStore, TreeSet<Integer> patientIds,
        @Value("${CHILD_CONCEPT_CACHE_SIZE:500}") int childConceptCacheSize
    ) {
        this(metaStore, patientIds, childConceptCacheSize, new HashMap<>());
    }

    public PhenotypeMetaStore(
        TreeMap<String, ColumnMeta> metaStore, TreeSet<Integer> patientIds,
        int childConceptCacheSize, Map<String, String> conceptToShardDir
    ) {
        this.metaStore = metaStore;
        this.patientIds = patientIds;
        this.conceptToShardDir = conceptToShardDir;

        this.childConceptCache = CacheBuilder.newBuilder().maximumSize(childConceptCacheSize).build(new CacheLoader<>() {
            @Override
            public Set<String> load(String key) {
                return loadChildConceptPaths(key);
            }
        });
    }
}

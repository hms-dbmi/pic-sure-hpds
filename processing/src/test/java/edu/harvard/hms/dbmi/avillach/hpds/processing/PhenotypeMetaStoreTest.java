package edu.harvard.hms.dbmi.avillach.hpds.processing;

import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.ColumnMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PhenotypeMetaStoreTest {

    private TreeMap<String, ColumnMeta> metaStore;

    private TreeSet<Integer> patientIds;

    private PhenotypeMetaStore phenotypeMetaStore;

    @BeforeEach
    public void setup() {
        metaStore = new TreeMap<>();
        metaStore.put("\\study1\\demographics\\age\\", new ColumnMeta().setName("age"));
        metaStore.put("\\study1\\demographics\\sex\\", new ColumnMeta().setName("sex"));
        metaStore.put("\\study2\\demographics\\age\\", new ColumnMeta().setName("age"));
        metaStore.put("\\study2\\demographics\\sex\\", new ColumnMeta().setName("sex"));

        patientIds = new TreeSet<>();
        phenotypeMetaStore = new PhenotypeMetaStore(metaStore, patientIds, 500);
    }

    @Test
    public void loadChildConceptPaths_matchingConcepts_shouldReturnConcepts() {
        Set<String> childConceptPaths = phenotypeMetaStore.loadChildConceptPaths("\\study1\\demographics\\");
        assertEquals(Set.of("\\study1\\demographics\\age\\", "\\study1\\demographics\\sex\\"), childConceptPaths);
    }

    @Test
    public void loadChildConceptPaths_noMatchingConcepts_shouldReturnNoConcepts() {
        Set<String> childConceptPaths = phenotypeMetaStore.loadChildConceptPaths("\\study3\\demographics\\");
        assertEquals(Set.of(), childConceptPaths);
    }

    @Test
    public void getChildConceptPaths_multipleCalls_shouldCacheResults() {
        TreeMap<String, ColumnMeta> metaStore = mock(TreeMap.class);
        when(metaStore.keySet()).thenReturn(
            Set.of(
                "\\study1\\demographics\\age\\", "\\study1\\demographics\\sex\\", "\\study2\\demographics\\age\\",
                "\\study2\\demographics\\sex\\"
            )
        );
        phenotypeMetaStore = new PhenotypeMetaStore(metaStore, patientIds, 500);

        for (int k = 0; k < 5; k++) {
            Set<String> childConceptPaths = phenotypeMetaStore.getChildConceptPaths("\\study1\\demographics\\");
            assertEquals(Set.of("\\study1\\demographics\\age\\", "\\study1\\demographics\\sex\\"), childConceptPaths);
        }
        verify(metaStore, times(1)).keySet();
    }

    @Test
    public void constructor_legacyMode_loadsFromRootDirectory(@TempDir File tempDir) throws Exception {
        TreeMap<String, ColumnMeta> meta = new TreeMap<>();
        meta.put("\\concept1\\", new ColumnMeta().setName("concept1"));
        meta.put("\\concept2\\", new ColumnMeta().setName("concept2"));
        TreeSet<Integer> ids = new TreeSet<>(Set.of(1, 2, 3));
        writeColumnMeta(new File(tempDir, "columnMeta.javabin"), meta, ids);

        PhenotypeMetaStore store = new PhenotypeMetaStore(tempDir.getAbsolutePath() + "/", 500);
        assertEquals(2, store.getMetaStore().size());
        assertTrue(store.getMetaStore().containsKey("\\concept1\\"));
        assertTrue(store.getMetaStore().containsKey("\\concept2\\"));
        assertEquals(3, store.getPatientIds().size());

        // All concepts should map to the root directory
        String rootDir = tempDir.getAbsolutePath() + "/";
        assertEquals(rootDir, store.getShardDirectory("\\concept1\\"));
        assertEquals(rootDir, store.getShardDirectory("\\concept2\\"));
    }

    @Test
    public void constructor_multiShardMode_mergesMetadata(@TempDir File tempDir) throws Exception {
        // Create shard1
        File shard1Dir = new File(tempDir, "shard_study1");
        shard1Dir.mkdirs();
        TreeMap<String, ColumnMeta> meta1 = new TreeMap<>();
        meta1.put("\\study1\\age\\", new ColumnMeta().setName("age"));
        TreeSet<Integer> ids1 = new TreeSet<>(Set.of(1, 2));
        writeColumnMeta(new File(shard1Dir, "columnMeta.javabin"), meta1, ids1);

        // Create shard2
        File shard2Dir = new File(tempDir, "shard_study2");
        shard2Dir.mkdirs();
        TreeMap<String, ColumnMeta> meta2 = new TreeMap<>();
        meta2.put("\\study2\\weight\\", new ColumnMeta().setName("weight"));
        TreeSet<Integer> ids2 = new TreeSet<>(Set.of(2, 3));
        writeColumnMeta(new File(shard2Dir, "columnMeta.javabin"), meta2, ids2);

        PhenotypeMetaStore store = new PhenotypeMetaStore(tempDir.getAbsolutePath() + "/", 500);
        assertEquals(2, store.getMetaStore().size());
        assertTrue(store.getMetaStore().containsKey("\\study1\\age\\"));
        assertTrue(store.getMetaStore().containsKey("\\study2\\weight\\"));
        // Patient IDs should be union
        assertEquals(new TreeSet<>(Set.of(1, 2, 3)), store.getPatientIds());

        // Concepts map to their respective shard directories
        assertEquals(shard1Dir.getAbsolutePath() + "/", store.getShardDirectory("\\study1\\age\\"));
        assertEquals(shard2Dir.getAbsolutePath() + "/", store.getShardDirectory("\\study2\\weight\\"));
    }

    @Test
    public void constructor_mixedMode_loadsBothLegacyAndShards(@TempDir File tempDir) throws Exception {
        // Legacy root file
        TreeMap<String, ColumnMeta> legacyMeta = new TreeMap<>();
        legacyMeta.put("\\legacy\\concept\\", new ColumnMeta().setName("legacy"));
        TreeSet<Integer> legacyIds = new TreeSet<>(Set.of(10));
        writeColumnMeta(new File(tempDir, "columnMeta.javabin"), legacyMeta, legacyIds);

        // Shard subdirectory
        File shardDir = new File(tempDir, "shard_new");
        shardDir.mkdirs();
        TreeMap<String, ColumnMeta> shardMeta = new TreeMap<>();
        shardMeta.put("\\new\\concept\\", new ColumnMeta().setName("new"));
        TreeSet<Integer> shardIds = new TreeSet<>(Set.of(20));
        writeColumnMeta(new File(shardDir, "columnMeta.javabin"), shardMeta, shardIds);

        PhenotypeMetaStore store = new PhenotypeMetaStore(tempDir.getAbsolutePath() + "/", 500);
        assertEquals(2, store.getMetaStore().size());
        assertTrue(store.getMetaStore().containsKey("\\legacy\\concept\\"));
        assertTrue(store.getMetaStore().containsKey("\\new\\concept\\"));
        assertEquals(new TreeSet<>(Set.of(10, 20)), store.getPatientIds());
    }

    @Test
    public void constructor_emptyDirectory_noFiles(@TempDir File tempDir) {
        PhenotypeMetaStore store = new PhenotypeMetaStore(tempDir.getAbsolutePath() + "/", 500);
        assertTrue(store.getMetaStore().isEmpty());
        assertTrue(store.getPatientIds().isEmpty());
    }

    @Test
    public void constructor_duplicateConcept_keepsFirst(@TempDir File tempDir) throws Exception {
        // Create shard1 with concept
        File shard1Dir = new File(tempDir, "shard_a");
        shard1Dir.mkdirs();
        TreeMap<String, ColumnMeta> meta1 = new TreeMap<>();
        ColumnMeta firstMeta = new ColumnMeta().setName("first").setObservationCount(100);
        meta1.put("\\shared\\concept\\", firstMeta);
        writeColumnMeta(new File(shard1Dir, "columnMeta.javabin"), meta1, new TreeSet<>(Set.of(1)));

        // Create shard2 with same concept
        File shard2Dir = new File(tempDir, "shard_b");
        shard2Dir.mkdirs();
        TreeMap<String, ColumnMeta> meta2 = new TreeMap<>();
        ColumnMeta secondMeta = new ColumnMeta().setName("second").setObservationCount(200);
        meta2.put("\\shared\\concept\\", secondMeta);
        writeColumnMeta(new File(shard2Dir, "columnMeta.javabin"), meta2, new TreeSet<>(Set.of(2)));

        PhenotypeMetaStore store = new PhenotypeMetaStore(tempDir.getAbsolutePath() + "/", 500);
        // Only one concept should exist
        assertEquals(1, store.getMetaStore().size());
        // First shard alphabetically wins (shard_a before shard_b)
        assertEquals(shard1Dir.getAbsolutePath() + "/", store.getShardDirectory("\\shared\\concept\\"));
    }

    @Test
    public void getShardDirectory_returnsCorrectPath() {
        Map<String, String> shardMap = Map.of(
            "\\concept1\\", "/data/shard1/",
            "\\concept2\\", "/data/shard2/"
        );
        PhenotypeMetaStore store = new PhenotypeMetaStore(new TreeMap<>(), new TreeSet<>(), 500, shardMap);
        assertEquals("/data/shard1/", store.getShardDirectory("\\concept1\\"));
        assertEquals("/data/shard2/", store.getShardDirectory("\\concept2\\"));
        assertNull(store.getShardDirectory("\\nonexistent\\"));
    }

    @Test
    public void constructor_emptySubdirectory_skipped(@TempDir File tempDir) throws Exception {
        // Create a subdirectory with no javabin files
        File emptySubDir = new File(tempDir, "empty_shard");
        emptySubDir.mkdirs();

        // Create a valid shard
        File validShardDir = new File(tempDir, "shard_valid");
        validShardDir.mkdirs();
        TreeMap<String, ColumnMeta> meta = new TreeMap<>();
        meta.put("\\valid\\concept\\", new ColumnMeta().setName("valid"));
        writeColumnMeta(new File(validShardDir, "columnMeta.javabin"), meta, new TreeSet<>(Set.of(1)));

        PhenotypeMetaStore store = new PhenotypeMetaStore(tempDir.getAbsolutePath() + "/", 500);
        assertEquals(1, store.getMetaStore().size());
        assertTrue(store.getMetaStore().containsKey("\\valid\\concept\\"));
    }

    private void writeColumnMeta(File file, TreeMap<String, ColumnMeta> meta, TreeSet<Integer> patientIds) throws Exception {
        try (ObjectOutputStream oos = new ObjectOutputStream(new GZIPOutputStream(new FileOutputStream(file)))) {
            oos.writeObject(meta);
            oos.writeObject(patientIds);
            oos.flush();
        }
    }
}

package edu.harvard.hms.dbmi.avillach.hpds.processing;

import edu.harvard.hms.dbmi.avillach.hpds.data.genotype.BucketIndexBySample;
import edu.harvard.hms.dbmi.avillach.hpds.data.genotype.VariantMasks;
import edu.harvard.hms.dbmi.avillach.hpds.data.genotype.VariantStore;
import edu.harvard.hms.dbmi.avillach.hpds.data.genotype.caching.VariantBucketHolder;
import edu.harvard.hms.dbmi.avillach.hpds.storage.FileBackedByteIndexedStorage;
import edu.harvard.hms.dbmi.avillach.hpds.storage.FileBackedJavaIndexedStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.*;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

@Service
public class VariantService {

    private static Logger log = LoggerFactory.getLogger(VariantService.class);

    private static final Integer VARIANT_INDEX_BLOCK_SIZE = 1000000;

    private final String genomicDataDirectory;

    private final String VARIANT_INDEX_FBBIS_STORAGE_FILE;
    private final String VARIANT_INDEX_FBBIS_FILE;
    private final String BUCKET_INDEX_BY_SAMPLE_FILE;


    private final VariantStore variantStore;

    // why is this not VariantSpec[]?
    private String[] variantIndex = null;
    private BucketIndexBySample bucketIndex;

    public String[] getVariantIndex() {
        return variantIndex;
    }

    public BucketIndexBySample getBucketIndex() {
        return bucketIndex;
    }
    public Collection<String> filterVariantSetForPatientSet(Set<String> variantSet, List<Integer> patientSet) {
        try {
            return bucketIndex.filterVariantSetForPatientSet(variantSet, patientSet);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public VariantService() throws IOException, ClassNotFoundException, InterruptedException {
        genomicDataDirectory = System.getProperty("HPDS_GENOMIC_DATA_DIRECTORY", "/opt/local/hpds/all/");
        VARIANT_INDEX_FBBIS_STORAGE_FILE = genomicDataDirectory + "variantIndex_fbbis_storage.javabin";
        VARIANT_INDEX_FBBIS_FILE = genomicDataDirectory + "variantIndex_fbbis.javabin";
        BUCKET_INDEX_BY_SAMPLE_FILE = genomicDataDirectory + "BucketIndexBySample.javabin";

        variantStore = loadVariantStore();
        try {
            loadGenomicCacheFiles();
        } catch (Exception e) {
            log.error("Failed to load genomic data: " + e.getLocalizedMessage(), e);
        }
    }

    private VariantStore loadVariantStore() {
        VariantStore variantStore;
        try {
            variantStore = VariantStore.readInstance(genomicDataDirectory);
        } catch (Exception e) {
            variantStore = new VariantStore();
            variantStore.setPatientIds(new String[0]);
            log.warn("Unable to load variant store");
        }
        return variantStore;
    }

    public String[] loadVariantIndex() {
        //skip if we have no variants
        if(variantStore.getPatientIds().length == 0) {
            log.warn("No Genomic Data found.  Skipping variant Indexing");
            return new String[0];
        }

        String[] variantIndex = VariantStore.loadVariantIndexFromFile(genomicDataDirectory);

        log.info("Index created with " + variantIndex.length + " total variants.");
        return variantIndex;
    }

    /**
     * This process takes a while (even after the cache is built), so let's spin it out into it's own thread. (not done yet)
     * @throws FileNotFoundException
     * @throws IOException
     * @throws InterruptedException
     */
    private void loadGenomicCacheFiles() throws FileNotFoundException, IOException, InterruptedException {
        if(bucketIndex==null) {
            if(variantIndex==null) {
                if(!new File(VARIANT_INDEX_FBBIS_FILE).exists()) {
                    log.info("Creating new " + VARIANT_INDEX_FBBIS_FILE);
                    this.variantIndex = loadVariantIndex();
                    FileBackedByteIndexedStorage<Integer, String[]> fbbis =
                            new FileBackedJavaIndexedStorage<>(Integer.class, String[].class, new File(VARIANT_INDEX_FBBIS_STORAGE_FILE));
                    try (ObjectOutputStream oos = new ObjectOutputStream(new GZIPOutputStream(new FileOutputStream(VARIANT_INDEX_FBBIS_FILE)));
                    ){

                        log.info("Writing Cache Object in blocks of " + VARIANT_INDEX_BLOCK_SIZE);

                        int bucketCount = (variantIndex.length / VARIANT_INDEX_BLOCK_SIZE) + 1;  //need to handle overflow
                        int index = 0;
                        for( int i = 0; i < bucketCount; i++) {
                            int blockSize = i == (bucketCount - 1) ? (variantIndex.length % VARIANT_INDEX_BLOCK_SIZE) : VARIANT_INDEX_BLOCK_SIZE;

                            String[] variantArrayBlock = new String[blockSize];
                            System.arraycopy(variantIndex, index, variantArrayBlock, 0, blockSize);
                            fbbis.put(i, variantArrayBlock);

                            index += blockSize;
                            log.info("saved " + index + " variants");
                        }
                        fbbis.complete();
                        oos.writeObject("" + variantIndex.length);
                        oos.writeObject(fbbis);
                        oos.flush();oos.close();
                    }
                }else {
                    ExecutorService ex = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
                    try (ObjectInputStream objectInputStream = new ObjectInputStream(new GZIPInputStream(new FileInputStream(VARIANT_INDEX_FBBIS_FILE)));){
                        Integer variantCount = Integer.parseInt((String) objectInputStream.readObject());
                        FileBackedByteIndexedStorage<Integer, String[]> indexStore = (FileBackedByteIndexedStorage<Integer, String[]>) objectInputStream.readObject();
                        log.info("loading " + VARIANT_INDEX_FBBIS_FILE);

                        variantIndex = new String[variantCount];
                        String[] _varaiantIndex2 = variantIndex;

                        //variant index has to be a single array (we use a binary search for lookups)
                        //but reading/writing to disk should be batched for performance
                        int bucketCount = (variantCount / VARIANT_INDEX_BLOCK_SIZE) + 1;  //need to handle overflow

                        for( int i = 0; i < bucketCount; i++) {
                            final int _i = i;
                            ex.submit(new Runnable() {
                                @Override
                                public void run() {
                                    try {
                                        String[] variantIndexBucket = indexStore.get(_i);
                                        System.arraycopy(variantIndexBucket, 0, _varaiantIndex2, (_i * VARIANT_INDEX_BLOCK_SIZE), variantIndexBucket.length);
                                        log.info("loaded " + (_i * VARIANT_INDEX_BLOCK_SIZE) + " block");
                                    } catch (IOException e) {
                                        throw new UncheckedIOException(e);
                                    }
                                }
                            });
                        }
                        objectInputStream.close();
                        ex.shutdown();
                        while(! ex.awaitTermination(60, TimeUnit.SECONDS)) {
                            System.out.println("Waiting for tasks to complete");
                            Thread.sleep(10000);
                        }
                    } catch (IOException | ClassNotFoundException | NumberFormatException e) {
                        log.error("an error occurred", e);
                    }
                    log.info("Found " + variantIndex.length + " total variants.");
                }
            }
            if(variantStore.getPatientIds().length > 0 && !new File(BUCKET_INDEX_BY_SAMPLE_FILE).exists()) {
                log.info("creating new " + BUCKET_INDEX_BY_SAMPLE_FILE);
                bucketIndex = new BucketIndexBySample(variantStore);
                try (
                        FileOutputStream fos = new FileOutputStream(BUCKET_INDEX_BY_SAMPLE_FILE);
                        GZIPOutputStream gzos = new GZIPOutputStream(fos);
                        ObjectOutputStream oos = new ObjectOutputStream(gzos);
                ){
                    oos.writeObject(bucketIndex);
                    oos.flush();oos.close();
                }
            }else {
                try (ObjectInputStream objectInputStream = new ObjectInputStream(new GZIPInputStream(new FileInputStream(BUCKET_INDEX_BY_SAMPLE_FILE)));){
                    log.info("loading " + BUCKET_INDEX_BY_SAMPLE_FILE);
                    bucketIndex = (BucketIndexBySample) objectInputStream.readObject();
                    objectInputStream.close();
                } catch (IOException | ClassNotFoundException e) {
                    log.error("an error occurred", e);
                }
            }
        }
    }

    public String[] getPatientIds() {
        return variantStore.getPatientIds();
    }

    public VariantMasks getMasks(String variantName, VariantBucketHolder<VariantMasks> bucketCache) {
        try {
            return variantStore.getMasks(variantName, bucketCache);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public BigInteger emptyBitmask() {
        return variantStore.emptyBitmask();
    }
}
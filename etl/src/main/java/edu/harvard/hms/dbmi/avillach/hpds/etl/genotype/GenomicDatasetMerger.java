package edu.harvard.hms.dbmi.avillach.hpds.etl.genotype;

import com.google.common.collect.Sets;
import edu.harvard.hms.dbmi.avillach.hpds.data.genotype.*;
import edu.harvard.hms.dbmi.avillach.hpds.data.storage.FileBackedStorageVariantMasksImpl;
import edu.harvard.hms.dbmi.avillach.hpds.storage.FileBackedByteIndexedStorage;
import edu.harvard.hms.dbmi.avillach.hpds.storage.FileBackedJsonIndexStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GenomicDatasetMerger {

    private static Logger log = LoggerFactory.getLogger(GenomicDatasetMerger.class);

    private final VariantStore variantStore1;
    private final VariantStore variantStore2;

    private final String genomicDirectory1;
    private final String genomicDirectory2;

    private final String outputDirectory;

    public GenomicDatasetMerger(String genomicDirectory1, String genomicDirectory2, String outputDirectory) throws IOException, ClassNotFoundException, InterruptedException {
        this.genomicDirectory1 = genomicDirectory1;
        this.genomicDirectory2 = genomicDirectory2;
        this.variantStore1 = VariantStore.deserializeInstance(genomicDirectory1);
        this.variantStore2 = VariantStore.deserializeInstance(genomicDirectory2);

        validate();
        this.outputDirectory = outputDirectory;
    }

    private void validate() {
        if (!variantStore1.getVariantMaskStorage().keySet().equals(variantStore2.getVariantMaskStorage().keySet())) {
            log.error("Variant store chromosomes do not match:");
            log.error(String.join(", ", variantStore1.getVariantMaskStorage().keySet()));
            log.error(String.join(", ", variantStore2.getVariantMaskStorage().keySet()));
            throw new IllegalStateException("Unable to merge variant stores with different numbers of chromosomes");
        }
    }

    /**
     * args[0]: directory containing genomic dataset 1
     * args[1]: directory containing genomic dataset 2
     * args[2]: output directory
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        long time = System.currentTimeMillis();
        GenomicDatasetMerger genomicDatasetMerger = new GenomicDatasetMerger(args[0], args[1], args[2]);
        genomicDatasetMerger.merge();
        log.info("Finished in " + (System.currentTimeMillis() - time) + " + ms");
    }

    public void merge() throws IOException {
        Map<String, FileBackedJsonIndexStorage<Integer, ConcurrentHashMap<String, VariantMasks>>> mergedChromosomeMasks = mergeChromosomeMasks();
        mergeVariantStore(mergedChromosomeMasks);
        Map<String, FileBackedByteIndexedInfoStore> mergedVariantIndexes = mergeVariantIndexes();
    }

    public void mergeVariantStore(Map<String, FileBackedJsonIndexStorage<Integer, ConcurrentHashMap<String, VariantMasks>>> mergedChromosomeMasks) {
        VariantStore mergedVariantStore = new VariantStore();
        mergedVariantStore.setVariantMaskStorage(mergedChromosomeMasks);
        mergedVariantStore.setPatientIds(mergePatientIds());
        // todo: duplicated from NewVCFLoader, refactor to common location
        try (FileOutputStream fos = new FileOutputStream(new File(outputDirectory, "variantStore.javabin"));
             GZIPOutputStream gzos = new GZIPOutputStream(fos);
             ObjectOutputStream oos = new ObjectOutputStream(gzos);) {
            oos.writeObject(mergedVariantStore);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Map<String, FileBackedByteIndexedInfoStore> mergeVariantIndexes() throws IOException {
        String[] variantIndex1 = VariantStore.loadVariantIndexFromFile(genomicDirectory1);
        String[] variantIndex2 = VariantStore.loadVariantIndexFromFile(genomicDirectory2);

        Map<String, Integer> variantSpecToIndexMap = new HashMap<>();
        LinkedList<String> variantSpecList = new LinkedList<>(Arrays.asList(variantIndex1));
        for (int i = 0; i < variantIndex1.length; i++) {
            variantSpecToIndexMap.put(variantIndex1[i], i);
        }

        // Will contain any re-mapped indexes in the second variant index. For example, if a variant is contained in both
        // data sets, the merged data set will use the index from dataset 1 to reference it, and any references in data
        // set 2 for this variant needs to be re-mapped. Likewise, if a variant in set 2 is new, it will be appended to
        // the list and also need to be re-mapped
        Integer[] remappedIndexes = new Integer[variantIndex2.length];

        for (int i = 0; i < variantIndex2.length; i++) {
            String variantSpec = variantIndex2[i];
            Integer variantIndex = variantSpecToIndexMap.get(variantSpec);
            if (variantIndex != null) {
                remappedIndexes[i] = variantIndex;
            } else {
                variantSpecList.add(variantSpec);
                // the new index is the now last item in the list
                int newVariantSpecIndex = variantSpecList.size() - 1;
                remappedIndexes[i] = newVariantSpecIndex;
                variantSpecToIndexMap.put(variantSpec, newVariantSpecIndex);
            }
        }

        Map<String, FileBackedByteIndexedInfoStore> infoStores1 = loadInfoStores(genomicDirectory1);
        Map<String, FileBackedByteIndexedInfoStore> infoStores2 = loadInfoStores(genomicDirectory2);
        Map<String, FileBackedByteIndexedInfoStore> mergedInfoStores = new HashMap<>();

        if (!infoStores1.keySet().equals(infoStores2.keySet())) {
            throw new IllegalStateException("Info stores do not match");
        }
        for (Map.Entry<String, FileBackedByteIndexedInfoStore> infoStores1Entry : infoStores1.entrySet()) {
            FileBackedByteIndexedInfoStore infoStore2 = infoStores2.get(infoStores1Entry.getKey());

            FileBackedByteIndexedStorage<String, String[]> allValuesStore1 = infoStores1Entry.getValue().getAllValues();
            FileBackedByteIndexedStorage<String, String[]> allValuesStore2 = infoStore2.getAllValues();
            //FileBackedByteIndexedStorage<String, String[]> mergedIndexedStorage = new FileBackedJavaIndexedStorage<>(String.class, String[].class, new File(outputDirectory));
            ConcurrentHashMap<String, ConcurrentSkipListSet<String>> mergedInfoStoreValues = new ConcurrentHashMap<>();

            Sets.SetView<String> allKeys = Sets.intersection(allValuesStore1.keys(), allValuesStore2.keys());
            for (String key : allKeys) {
                Set<String> store1Values = new HashSet<>(Arrays.asList(allValuesStore1.getOrELse(key, new String[]{})));
                Set<String> store2Values = new HashSet<>(Arrays.asList(allValuesStore2.getOrELse(key, new String[]{})));
                Set<String> remappedValuesStore2 = store2Values.stream().map(Integer::parseInt).map(value -> remappedIndexes[value]).map(Object::toString).collect(Collectors.toSet());

                Set<String> mergedValues = Sets.union(store1Values, remappedValuesStore2);
                mergedInfoStoreValues.put(key, new ConcurrentSkipListSet<>(mergedValues));
            }

            InfoStore infoStore = new InfoStore(infoStore2.description, null, infoStores1Entry.getKey());
            infoStore.allValues = mergedInfoStoreValues;
            FileBackedByteIndexedInfoStore mergedStore = new FileBackedByteIndexedInfoStore(new File(outputDirectory), infoStore);
            mergedInfoStores.put(infoStores1Entry.getKey(), mergedStore);
            mergedStore.write(new File(outputDirectory + infoStore.column_key + "_infoStore.javabin"));
        }

        try (FileOutputStream fos = new FileOutputStream(new File(outputDirectory, "variantSpecIndex.javabin"));
             GZIPOutputStream gzos = new GZIPOutputStream(fos);
             ObjectOutputStream oos = new ObjectOutputStream(gzos);) {
            oos.writeObject(variantSpecList);
        }

        return mergedInfoStores;
    }

    private Map<String, FileBackedByteIndexedInfoStore> loadInfoStores(String directory) {
        Map<String, FileBackedByteIndexedInfoStore> infoStores = new HashMap<>();
        File genomicDataDirectory = new File(directory);
        if(genomicDataDirectory.exists() && genomicDataDirectory.isDirectory()) {
            Arrays.stream(genomicDataDirectory.list((file, filename)->{return filename.endsWith("infoStore.javabin");}))
                    .forEach((String filename)->{
                        try (
                                FileInputStream fis = new FileInputStream(directory + filename);
                                GZIPInputStream gis = new GZIPInputStream(fis);
                                ObjectInputStream ois = new ObjectInputStream(gis)
                        ){
                            log.info("loading " + filename);
                            FileBackedByteIndexedInfoStore infoStore = (FileBackedByteIndexedInfoStore) ois.readObject();
                            infoStore.updateStorageDirectory(genomicDataDirectory);
                            infoStores.put(filename.replace("_infoStore.javabin", ""), infoStore);
                            ois.close();
                        } catch (IOException | ClassNotFoundException e) {
                            e.printStackTrace();
                        }
                    });
        }
        return infoStores;
    }

    private String[] mergePatientIds() {
        return Stream.concat(Arrays.stream(variantStore1.getPatientIds()), Arrays.stream(variantStore2.getPatientIds()))
                .toArray(String[]::new);
    }

    public Map<String, FileBackedJsonIndexStorage<Integer, ConcurrentHashMap<String, VariantMasks>>> mergeChromosomeMasks() throws FileNotFoundException {
        Map<String, FileBackedJsonIndexStorage<Integer, ConcurrentHashMap<String, VariantMasks>>> mergedMaskStorage = new HashMap<>();
        for (String chromosome : variantStore1.getVariantMaskStorage().keySet()) {
            mergedMaskStorage.put(chromosome, mergeChromosomeMask(chromosome));
        }
        return mergedMaskStorage;
    }

    public FileBackedJsonIndexStorage<Integer, ConcurrentHashMap<String, VariantMasks>> mergeChromosomeMask(String chromosome) throws FileNotFoundException {
        FileBackedJsonIndexStorage<Integer, ConcurrentHashMap<String, VariantMasks>> variantMaskStorage1 = variantStore1.getVariantMaskStorage().get(chromosome);
        FileBackedJsonIndexStorage<Integer, ConcurrentHashMap<String, VariantMasks>> variantMaskStorage2 = variantStore2.getVariantMaskStorage().get(chromosome);

        FileBackedJsonIndexStorage<Integer, ConcurrentHashMap<String, VariantMasks>> merged = new FileBackedStorageVariantMasksImpl(new File(outputDirectory + chromosome + "masks.bin"));
        variantMaskStorage1.keys().forEach(key -> {
            try {
                Map<String, VariantMasks> masks1 = variantMaskStorage1.get(key);
                Map<String, VariantMasks> masks2 = variantMaskStorage2.get(key);
                if (masks2 == null) {
                    masks2 = Map.of();
                }

                ConcurrentHashMap<String, VariantMasks> mergedMasks = new ConcurrentHashMap<>();
                for (Map.Entry<String, VariantMasks> entry : masks1.entrySet()) {
                    VariantMasks variantMasks2 = masks2.get(entry.getKey());
                    if (variantMasks2 == null) {
                        // this will have all null masks, which will result in null when
                        // appended to a null, or be replaced with an empty bitmask otherwise
                        variantMasks2 = new VariantMasks();
                    }
                    mergedMasks.put(entry.getKey(), append(entry.getValue(), variantMasks2));
                }
                // Any entry in the second set that is not in the merged set can be merged with an empty variant mask,
                // if there were a corresponding entry in set 1, it would have been merged in the previous loop
                for (Map.Entry<String, VariantMasks> entry : masks2.entrySet()) {
                    if (!mergedMasks.containsKey(entry.getKey())) {
                        mergedMasks.put(entry.getKey(), append(new VariantMasks(), entry.getValue()));
                    }
                }
                merged.put(key, mergedMasks);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        ConcurrentHashMap<String, VariantMasks> mergedMasks = new ConcurrentHashMap<>();
        variantMaskStorage2.keys().forEach(key -> {
            try {
                Map<String, VariantMasks> masks2 = variantMaskStorage2.get(key);
                for (Map.Entry<String, VariantMasks> entry : masks2.entrySet()) {
                    if (!mergedMasks.containsKey(entry.getKey())) {
                        mergedMasks.put(entry.getKey(), append(new VariantMasks(), entry.getValue()));
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        return merged;
    }

    public VariantMasks append(VariantMasks variantMasks1, VariantMasks variantMasks2) {
        VariantMasks appendedMasks = new VariantMasks();
        appendedMasks.homozygousMask = appendMask(variantMasks1.homozygousMask, variantMasks2.homozygousMask);
        appendedMasks.heterozygousMask = appendMask(variantMasks1.heterozygousMask, variantMasks2.heterozygousMask);
        appendedMasks.homozygousNoCallMask = appendMask(variantMasks1.homozygousNoCallMask, variantMasks2.homozygousNoCallMask);
        appendedMasks.heterozygousNoCallMask = appendMask(variantMasks1.heterozygousNoCallMask, variantMasks2.heterozygousNoCallMask);
        return appendedMasks;
    }

    /**
     * Appends one mask to another. This assumes the masks are both padded with '11' on each end
     * to prevent overflow issues.
     */
    public BigInteger appendMask(BigInteger mask1, BigInteger mask2) {
        if (mask1 == null && mask2 == null) {
            return null;
        }
        if (mask1 == null) {
            mask1 = variantStore1.emptyBitmask();
        }
        if (mask2 == null) {
            mask2 = variantStore2.emptyBitmask();
        }
        String binaryMask1 = mask1.toString(2);
        String binaryMask2 = mask2.toString(2);
        String appendedString = binaryMask1.substring(0, binaryMask1.length() - 2) +
                binaryMask2.substring(2);
        return new BigInteger(appendedString, 2);
    }
}

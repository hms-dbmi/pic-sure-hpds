package edu.harvard.hms.dbmi.avillach.hpds.processing;

import com.google.common.base.Joiner;
import com.google.common.collect.Range;
import edu.harvard.hms.dbmi.avillach.hpds.data.genotype.*;
import edu.harvard.hms.dbmi.avillach.hpds.data.genotype.caching.VariantBucketHolder;
import edu.harvard.hms.dbmi.avillach.hpds.data.query.Filter;
import edu.harvard.hms.dbmi.avillach.hpds.data.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import static edu.harvard.hms.dbmi.avillach.hpds.data.genotype.VariantMetadataIndex.VARIANT_METADATA_FILENAME;

public class GenomicProcessorNodeImpl implements GenomicProcessor {

    private static Logger log = LoggerFactory.getLogger(GenomicProcessorNodeImpl.class);

    private final PatientVariantJoinHandler patientVariantJoinHandler;

    private final VariantIndexCache variantIndexCache;

    private final Map<String, FileBackedByteIndexedInfoStore> infoStores;

    private final Set<String> infoStoreColumns;

    private final String genomicDataDirectory;

    private final VariantService variantService;


    private final String HOMOZYGOUS_VARIANT = "1/1";
    private final String HETEROZYGOUS_VARIANT = "0/1";
    private final String HOMOZYGOUS_REFERENCE = "0/0";

    public GenomicProcessorNodeImpl(String genomicDataDirectory) {
        this.genomicDataDirectory = genomicDataDirectory;
        this.variantService = new VariantService(genomicDataDirectory);
        this.patientVariantJoinHandler = new PatientVariantJoinHandler(variantService);

        infoStores = new HashMap<>();
        File genomicDataDirectoryFile = new File(this.genomicDataDirectory);
        if(genomicDataDirectoryFile.exists() && genomicDataDirectoryFile.isDirectory()) {
            Arrays.stream(genomicDataDirectoryFile.list((file, filename)->{return filename.endsWith("infoStore.javabin");}))
                    .forEach((String filename)->{
                        try (
                                FileInputStream fis = new FileInputStream(this.genomicDataDirectory + filename);
                                GZIPInputStream gis = new GZIPInputStream(fis);
                                ObjectInputStream ois = new ObjectInputStream(gis)
                        ){
                            log.info("loading " + filename);
                            FileBackedByteIndexedInfoStore infoStore = (FileBackedByteIndexedInfoStore) ois.readObject();
                            infoStore.updateStorageDirectory(genomicDataDirectoryFile);
                            infoStores.put(filename.replace("_infoStore.javabin", ""), infoStore);
                            ois.close();
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        } catch (ClassNotFoundException e) {
                            throw new RuntimeException(e);
                        }
                    });
        } else {
            throw new IllegalArgumentException("Not a valid genomicDataDirectory: " + this.genomicDataDirectory);
        }
        infoStoreColumns = new HashSet<>(infoStores.keySet());

        variantIndexCache = new VariantIndexCache(variantService.getVariantIndex(), infoStores);
    }

    @Override
    public Mono<VariantMask> getPatientMask(DistributableQuery distributableQuery) {
        return Mono.fromCallable(() -> runGetPatientMask(distributableQuery)).subscribeOn(Schedulers.boundedElastic());
    }
    public VariantMask runGetPatientMask(DistributableQuery distributableQuery) {
        /* VARIANT INFO FILTER HANDLING IS MESSY */
        if(distributableQuery.hasFilters()) {
            VariantIndex intersectionOfInfoFilters = null;
            for(Query.VariantInfoFilter filter : distributableQuery.getVariantInfoFilters()){
                List<VariantIndex> variantSets = getVariantsMatchingFilters(filter);
                log.debug("Found " + variantSets.size() + " groups of sets for patient identification");
                if(!variantSets.isEmpty()) {
                    // INTERSECT all the variant sets.
                    if (intersectionOfInfoFilters == null) {
                        intersectionOfInfoFilters = variantSets.get(0);
                    }
                    for(VariantIndex variantSet : variantSets) {
                        intersectionOfInfoFilters = intersectionOfInfoFilters.intersection(variantSet);
                    }
                } else {
                    intersectionOfInfoFilters = VariantIndex.empty();
                }
            }

            // todo: handle empty getVariantInfoFilters()

            // add filteredIdSet for patients who have matching variants, heterozygous or homozygous for now.
            VariantMask patientMask = null;
            if (intersectionOfInfoFilters != null ){
                patientMask = patientVariantJoinHandler.getPatientIdsForIntersectionOfVariantSets(distributableQuery.getPatientIds(), intersectionOfInfoFilters);
            } else {
                patientMask = createMaskForPatientSet(distributableQuery.getPatientIds());
            }

            for (String snp : distributableQuery.getCategoryFilters().keySet()) {
                VariantMask patientsForVariantSpec = getVariantIndexesForSpec(distributableQuery.getCategoryFilters().get(snp), snp);
                if (patientMask == null) {
                    patientMask = patientsForVariantSpec;
                } else {
                    patientMask = patientMask.intersection(patientsForVariantSpec);
                }
            }

            VariantBucketHolder<VariableVariantMasks> variantMasksVariantBucketHolder = new VariantBucketHolder<>();
            if (!distributableQuery.getRequiredFields().isEmpty() ) {
                for (String variantSpec : distributableQuery.getRequiredFields()) {
                    VariantMask patientsForVariantSpec = getVariantIndexesForSpec(new String[]{"0/1", "1/1"}, variantSpec);
                    if (patientMask == null) {
                        patientMask = patientsForVariantSpec;
                    } else {
                        patientMask = patientMask.intersection(patientsForVariantSpec);
                    }
                }
            }

            return patientMask;
        }
        return createMaskForPatientSet(distributableQuery.getPatientIds());
        /* END OF VARIANT INFO FILTER HANDLING */
    }

    private VariantMask getVariantIndexesForSpec(String[] zygosities, String variantSpec) {
        List<VariableVariantMasks> masksForDbSnpSpec = variantService.getMasksForDbSnpSpec(variantSpec);
        VariantMask variantMask = VariantMask.emptyInstance();

        for (VariableVariantMasks masks : masksForDbSnpSpec) {
            for (String zygosity : zygosities) {
                if(zygosity.equals(HOMOZYGOUS_REFERENCE)) {
                    /*// todo: implement this -- difficult with sparse variants of unknown length
                    VariantMask homozygousReferenceBitmask = calculateIndiscriminateBitmask(masks);
                    for(int x = 2;x<homozygousReferenceBitmask.bitLength()-2;x++) {
                        homozygousReferenceBitmask = homozygousReferenceBitmask.flipBit(x);
                    }
                    variantBitmasks.add(homozygousReferenceBitmask);*/
                } else if(masks.heterozygousMask != null && zygosity.equals(HETEROZYGOUS_VARIANT)) {
                    variantMask = variantMask.union(masks.heterozygousMask);
                } else if(masks.homozygousMask != null && zygosity.equals(HOMOZYGOUS_VARIANT)) {
                    variantMask = variantMask.union(masks.homozygousMask);
                } else if(zygosity.equals("")) {
                    if (masks.homozygousMask != null) {
                        variantMask = variantMask.union(masks.homozygousMask);
                    }
                    if (masks.heterozygousMask != null) {
                        variantMask = variantMask.union(masks.heterozygousMask);
                    }
                }
            }
        }
        return variantMask;
    }

    @Override
    public Set<Integer> patientMaskToPatientIdSet(VariantMask patientMask) {
        return patientMask.patientMaskToPatientIdSet(getPatientIds());
    }

    private List<VariantIndex> getVariantsMatchingFilters(Query.VariantInfoFilter filter) {
        List<VariantIndex> variantIndices = new ArrayList<>();
        // Add variant sets for each filter
        if(filter.categoryVariantInfoFilters != null && !filter.categoryVariantInfoFilters.isEmpty()) {
            filter.categoryVariantInfoFilters.entrySet().stream().forEach((Map.Entry<String,String[]> entry) ->{
                variantIndices.add(getComputedVariantIndexForCategoryFilter(entry));
            });
        }
        if(filter.numericVariantInfoFilters != null && !filter.numericVariantInfoFilters.isEmpty()) {
            filter.numericVariantInfoFilters.forEach((String column, Filter.FloatFilter doubleFilter)->{
                Optional<FileBackedByteIndexedInfoStore> infoStoreOptional = getInfoStore(column);

                doubleFilter.getMax();
                Range<Float> filterRange = Range.closed(doubleFilter.getMin(), doubleFilter.getMax());
                infoStoreOptional.ifPresentOrElse(infoStore -> {
                    List<String> valuesInRange = infoStore.continuousValueIndex.getValuesInRange(filterRange);
                    for(String value : valuesInRange) {
                        variantIndices.add(variantIndexCache.get(column, value));
                    }},
                    () -> {
                        variantIndices.add(VariantIndex.empty());
                    }
                );

            });
        }
        return variantIndices;
    }

    private VariantIndex getComputedVariantIndexForCategoryFilter(Map.Entry<String, String[]> entry) {
        String column = entry.getKey();
        String[] values = entry.getValue();
        Arrays.sort(values);
        Optional<FileBackedByteIndexedInfoStore> infoStoreOptional = getInfoStore(column);

        List<String> infoKeys = infoStoreOptional.map(infoStore -> filterInfoCategoryKeys(values, infoStore))
                .orElseGet(ArrayList::new);

        if(infoKeys.size()>1) {
            // These should be ANDed
            return infoKeys.stream()
                    .map(key -> variantIndexCache.get(column, key))
                    .reduce(VariantIndex::union)
                    .orElseGet(() -> {
                        log.warn("No variant index computed for category filter. This should never happen");
                        return VariantIndex.empty();
                    });
        } else if(infoKeys.size() == 1) {
            return variantIndexCache.get(column, infoKeys.get(0));
        } else { // infoKeys.size() == 0
            log.debug("No indexes found for column [" + column + "] for values [" + Joiner.on(",").join(values) + "]");
            return VariantIndex.empty();
        }
    }

    private List<String> filterInfoCategoryKeys(String[] values, FileBackedByteIndexedInfoStore infoStore) {
        List<String> infoKeys = infoStore.getAllValues().keys().stream().filter((String key)->{
            // iterate over the values for the specific category and find which ones match the search
            int insertionIndex = Arrays.binarySearch(values, key);
            return insertionIndex > -1 && insertionIndex < values.length;
        }).collect(Collectors.toList());
        log.debug("found " + infoKeys.size() + " keys for info category filters");
        return infoKeys;
    }

    @Override
    public VariantMask createMaskForPatientSet(Set<Integer> patientSubset) {
        return new VariantMaskBitmaskImpl(patientVariantJoinHandler.createMaskForPatientSet(patientSubset));
    }

    private Optional<FileBackedByteIndexedInfoStore> getInfoStore(String column) {
        return Optional.ofNullable(infoStores.get(column));
    }

    private VariantIndex addVariantsForInfoFilter(VariantIndex unionOfInfoFilters, Query.VariantInfoFilter filter) {
        List<VariantIndex> variantSets = getVariantsMatchingFilters(filter);

        if(!variantSets.isEmpty()) {
            VariantIndex intersectionOfInfoFilters = variantSets.get(0);
            for(VariantIndex variantSet : variantSets) {
                intersectionOfInfoFilters = intersectionOfInfoFilters.intersection(variantSet);
            }
            unionOfInfoFilters = unionOfInfoFilters.union(intersectionOfInfoFilters);
        } else {
            log.warn("No info filters included in query.");
        }
        return unionOfInfoFilters;
    }

    @Override
    public Mono<Set<String>> getVariantList(DistributableQuery query) {
        return Mono.fromCallable(() -> runGetVariantList(query)).subscribeOn(Schedulers.boundedElastic());
    }
    public Set<String> runGetVariantList(DistributableQuery query) {
        boolean queryContainsVariantInfoFilters = query.getVariantInfoFilters().stream().anyMatch(variantInfoFilter ->
                !variantInfoFilter.categoryVariantInfoFilters.isEmpty() || !variantInfoFilter.numericVariantInfoFilters.isEmpty()
        );
        if(queryContainsVariantInfoFilters) {
            VariantIndex unionOfInfoFilters = VariantIndex.empty();

            for(Query.VariantInfoFilter filter : query.getVariantInfoFilters()){
                unionOfInfoFilters = addVariantsForInfoFilter(unionOfInfoFilters, filter);
            }

            VariantMask patientMask = runGetPatientMask(query);

            // If we have all patients then no variants would be filtered, so no need to do further processing
            /*if(patientSubset.size()==variantService.getPatientIds().length) {
                log.info("query selects all patient IDs, returning....");
                return unionOfInfoFilters.mapToVariantSpec(variantService.getVariantIndex());
            }*/

            Set<String> unionOfInfoFiltersVariantSpecs = unionOfInfoFilters.mapToVariantSpec(variantService.getVariantIndex());
            Collection<String> variantsInScope = variantService.filterVariantSetForPatientSet(unionOfInfoFiltersVariantSpecs, patientMaskToPatientIdSet(patientMask));

            //NC - this is the original variant filtering, which checks the patient mask from each variant against the patient mask from the query
            if(variantsInScope.size()<100000) {
                ConcurrentSkipListSet<String> variantsWithPatients = new ConcurrentSkipListSet<>();
                variantsInScope.parallelStream().forEach(variantKey -> {
                    variantService.getMasks(variantKey, new VariantBucketHolder<>()).ifPresent(masks -> {
                        if ( masks.heterozygousMask != null && !masks.heterozygousMask.intersection(patientMask).isEmpty()) {
                            variantsWithPatients.add(variantKey);
                        } else if ( masks.homozygousMask != null && !masks.homozygousMask.intersection(patientMask).isEmpty()) {
                            variantsWithPatients.add(variantKey);
                        } else if ( masks.heterozygousNoCallMask != null && !masks.heterozygousNoCallMask.intersection(patientMask).isEmpty()) {
                            //so heterozygous no calls we want, homozygous no calls we don't
                            variantsWithPatients.add(variantKey);
                        }
                    });
                });
                return variantsWithPatients;
            }else {
                return unionOfInfoFiltersVariantSpecs;
            }
        }
        return new HashSet<>();
    }

    private VariantMask getIdSetForVariantSpecCategoryFilter(String[] zygosities, String key, VariantBucketHolder<VariableVariantMasks> bucketCache) {
        List<VariantMask> variantBitmasks = getBitmasksForVariantSpecCategoryFilter(zygosities, key, bucketCache);
        Set<Integer> patientIds = new HashSet<>();
        if(!variantBitmasks.isEmpty()) {
            VariantMask variantMask = variantBitmasks.get(0);
            if(variantBitmasks.size()>1) {
                for(int x = 1;x<variantBitmasks.size();x++) {
                    variantMask = variantMask.union(variantBitmasks.get(x));
                }
            }
            return variantMask;
        }
        return VariantMask.emptyInstance();
    }

    private ArrayList<VariantMask> getBitmasksForVariantSpecCategoryFilter(String[] zygosities, String variantName, VariantBucketHolder<VariableVariantMasks> bucketCache) {
        ArrayList<VariantMask> variantBitmasks = new ArrayList<>();
        variantName = variantName.replaceAll(",\\d/\\d$", "");
        log.debug("looking up mask for : " + variantName);
        Optional<VariableVariantMasks> optionalMasks = variantService.getMasks(variantName, bucketCache);
        Arrays.stream(zygosities).forEach((zygosity) -> {
            optionalMasks.ifPresent(masks -> {
                if(zygosity.equals(HOMOZYGOUS_REFERENCE)) {
                    // todo: implement for VariantMask. I don't think this logic was sound previously
                    /*VariantMask homozygousReferenceBitmask = calculateIndiscriminateBitmask(masks);
                    for(int x = 2;x<homozygousReferenceBitmask.bitLength()-2;x++) {
                        homozygousReferenceBitmask = homozygousReferenceBitmask.flipBit(x);
                    }
                    variantBitmasks.add(homozygousReferenceBitmask);*/
                } else if(masks.heterozygousMask != null && zygosity.equals(HETEROZYGOUS_VARIANT)) {
                    variantBitmasks.add(masks.heterozygousMask);
                }else if(masks.homozygousMask != null && zygosity.equals(HOMOZYGOUS_VARIANT)) {
                    variantBitmasks.add(masks.homozygousMask);
                }else if(zygosity.equals("")) {
                    variantBitmasks.add(calculateIndiscriminateBitmask(masks));
                }
            });
            if (optionalMasks.isEmpty()) {
                variantBitmasks.add(new VariantMaskBitmaskImpl(variantService.emptyBitmask()));
            }

        });
        return variantBitmasks;
    }

    /**
     * Calculate a bitmask which is a bitwise OR of any populated masks in the VariantMasks passed in
     * @param masks
     * @return
     */
    private VariantMask calculateIndiscriminateBitmask(VariableVariantMasks masks) {
        VariantMask indiscriminateVariantBitmask = null;
        if(masks.heterozygousMask == null && masks.homozygousMask != null) {
            indiscriminateVariantBitmask = masks.homozygousMask;
        }else if(masks.homozygousMask == null && masks.heterozygousMask != null) {
            indiscriminateVariantBitmask = masks.heterozygousMask;
        }else if(masks.homozygousMask != null && masks.heterozygousMask != null) {
            indiscriminateVariantBitmask = masks.heterozygousMask.intersection(masks.homozygousMask);
        }else {
            indiscriminateVariantBitmask = new VariantMaskBitmaskImpl(variantService.emptyBitmask());
        }
        return indiscriminateVariantBitmask;
    }

    @Override
    public List<String> getPatientIds() {
        return List.of(variantService.getPatientIds());
    }

    @Override
    public Optional<VariableVariantMasks> getMasks(String path, VariantBucketHolder<VariableVariantMasks> variantMasksVariantBucketHolder) {
        return variantService.getMasks(path, variantMasksVariantBucketHolder);
    }

    @Override
    public Set<String> getInfoStoreColumns() {
        return infoStoreColumns;
    }

    @Override
    public Set<String> getInfoStoreValues(String conceptPath) {
        return infoStores.get(conceptPath).getAllValues().keys()
                .stream()
                .sorted(String::compareToIgnoreCase)
                .collect(Collectors.toSet());
    }

    @Override
    public List<InfoColumnMeta> getInfoColumnMeta() {
        return getInfoStoreColumns().stream().map(infoStores::get)
                .map(fileBackedByteIndexedInfoStore -> new InfoColumnMeta(
                        fileBackedByteIndexedInfoStore.column_key,
                        fileBackedByteIndexedInfoStore.description,
                        fileBackedByteIndexedInfoStore.isContinuous,
                        fileBackedByteIndexedInfoStore.min,
                        fileBackedByteIndexedInfoStore.max
                    )
                )
                .collect(Collectors.toList());
    }

    @Override
    public Map<String, Set<String>> getVariantMetadata(Collection<String> variantList) {
        return variantService.findByMultipleVariantSpec(variantList);
    }
}

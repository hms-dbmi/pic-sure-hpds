package edu.harvard.hms.dbmi.avillach.hpds.processing;

import java.io.*;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Joiner;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.*;
import com.google.common.cache.CacheLoader.InvalidCacheLoadException;
import com.google.common.collect.Sets;

import edu.harvard.hms.dbmi.avillach.hpds.crypto.Crypto;
import edu.harvard.hms.dbmi.avillach.hpds.data.genotype.*;
import edu.harvard.hms.dbmi.avillach.hpds.data.genotype.caching.VariantBucketHolder;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.ColumnMeta;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.PhenoCube;
import edu.harvard.hms.dbmi.avillach.hpds.data.query.Query;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;


@Component
public class AbstractProcessor {

	private static Logger log = LoggerFactory.getLogger(AbstractProcessor.class);

	private final String ID_CUBE_NAME;
	private final int ID_BATCH_SIZE;
	private final int CACHE_SIZE;

	private final String hpdsDataDirectory;


	@Value("${HPDS_GENOMIC_DATA_DIRECTORY:/opt/local/hpds/all/}")
	private String hpdsGenomicDataDirectory;

	private LoadingCache<String, PhenoCube<?>> store;

	private final PhenotypeMetaStore phenotypeMetaStore;

	private final GenomicProcessor genomicProcessor;


	@Autowired
	public AbstractProcessor(
			PhenotypeMetaStore phenotypeMetaStore,
			GenomicProcessor genomicProcessor, @Value("${HPDS_DATA_DIRECTORY:/opt/local/hpds/}") String hpdsDataDirectory
	) throws ClassNotFoundException, IOException, InterruptedException {

		this.hpdsDataDirectory = hpdsDataDirectory;
		this.phenotypeMetaStore = phenotypeMetaStore;
		this.genomicProcessor = genomicProcessor;

		CACHE_SIZE = Integer.parseInt(System.getProperty("CACHE_SIZE", "100"));
		ID_BATCH_SIZE = Integer.parseInt(System.getProperty("ID_BATCH_SIZE", "0"));
		ID_CUBE_NAME = System.getProperty("ID_CUBE_NAME", "NONE");

		store = initializeCache();

		if(Crypto.hasKey(Crypto.DEFAULT_KEY_NAME)) {
			List<String> cubes = new ArrayList<String>(phenotypeMetaStore.getColumnNames());
			int conceptsToCache = Math.min(cubes.size(), CACHE_SIZE);
			for(int x = 0;x<conceptsToCache;x++){
				try {
					if(phenotypeMetaStore.getColumnMeta(cubes.get(x)).getObservationCount() == 0){
						log.info("Rejecting : " + cubes.get(x) + " because it has no entries.");
					}else {
						store.get(cubes.get(x));
						log.debug("loaded: " + cubes.get(x));
						// +1 offset when logging to print _after_ each 10%
						if((x + 1) % (conceptsToCache * .1)== 0) {
							log.info("cached: " + (x + 1) + " out of " + conceptsToCache);
						}
					}
				} catch (ExecutionException e) {
					log.error("an error occurred", e);
				}

			}

		}
	}

	public AbstractProcessor(PhenotypeMetaStore phenotypeMetaStore, LoadingCache<String, PhenoCube<?>> store,
							 Map<String, FileBackedByteIndexedInfoStore> infoStores, List<String> infoStoreColumns,
							 GenomicProcessor genomicProcessor, String hpdsDataDirectory) {
		this.phenotypeMetaStore = phenotypeMetaStore;
		this.store = store;
		this.genomicProcessor = genomicProcessor;

		CACHE_SIZE = Integer.parseInt(System.getProperty("CACHE_SIZE", "100"));
		ID_BATCH_SIZE = Integer.parseInt(System.getProperty("ID_BATCH_SIZE", "0"));
		ID_CUBE_NAME = System.getProperty("ID_CUBE_NAME", "NONE");

		this.hpdsDataDirectory = hpdsDataDirectory;
	}

	public Set<String> getInfoStoreColumns() {
		return genomicProcessor.getInfoStoreColumns();
	}


	/**
	 * Merges a list of sets of patient ids by intersection. If we implemented OR semantics
	 * this would be where the change happens.
	 *
	 * @param filteredIdSets
	 * @return
	 */
	protected Set<Integer> applyBooleanLogic(List<Set<Integer>> filteredIdSets) {
		Set<Integer>[] ids = new Set[] {filteredIdSets.get(0)};
		filteredIdSets.forEach((keySet)->{
			// I believe this is not ideal for large numbers of sets. Sets.intersection creates a view of the 2 sets, so
			// doing this repeatedly would create views on views on views of the backing sets. retainAll() returns a new
			// set with only the common elements, and if we sort by set size and start with the smallest I believe that
			// will be more efficient
			ids[0] = Sets.intersection(ids[0], keySet);
		});
		return ids[0];
	}

	/**
	 *
	 * @param query
	 * @return
	 */
	protected Set<Integer> idSetsForEachFilter(Query query) {
		DistributableQuery distributableQuery = getDistributableQuery(query);

		// NULL (representing no phenotypic filters, i.e. all patients) or not empty patient ID sets require a genomic query.
		// Otherwise, short circuit and return no patients
		if ((distributableQuery.getPatientIds() == null || !distributableQuery.getPatientIds().isEmpty()) && distributableQuery.hasFilters()) {
            Mono<VariantMask> patientMaskForVariantInfoFilters = genomicProcessor.getPatientMask(distributableQuery);
			return patientMaskForVariantInfoFilters.map(genomicProcessor::patientMaskToPatientIdSet).block();
        }
		return distributableQuery.getPatientIds();
	}

	private DistributableQuery getDistributableQuery(Query query) {
		DistributableQuery distributableQuery = new DistributableQuery();
		List<Set<Integer>> patientIdSets = new ArrayList<>();

		try {
			query.getAllAnyRecordOf().forEach(anyRecordOfFilterList -> {
				getPatientIdsForAnyRecordOf(anyRecordOfFilterList, distributableQuery).map(patientIdSets::add);
			});
            patientIdSets.addAll(getIdSetsForNumericFilters(query));
            patientIdSets.addAll(getIdSetsForRequiredFields(query, distributableQuery));
            patientIdSets.addAll(getIdSetsForCategoryFilters(query, distributableQuery));
		} catch (InvalidCacheLoadException e) {
			log.warn("Invalid query supplied: " + e.getLocalizedMessage());
			patientIdSets.add(new HashSet<>()); // if an invalid path is supplied, no patients should match.
		}

		Set<Integer> phenotypicPatientSet = null;
		//AND logic to make sure all patients match each filter
		if(!patientIdSets.isEmpty()) {
			phenotypicPatientSet = applyBooleanLogic(patientIdSets);
		} else {
            // if there are no patient filters, represent with null. 0 patients means no patients matched the filter
		}
		distributableQuery.setVariantInfoFilters(query.getVariantInfoFilters());
		distributableQuery.setPatientIds(phenotypicPatientSet);
		return distributableQuery;
	}

	/**
	 * Process each filter in the query and return a list of patient ids that should be included in the
	 * result.
	 *
	 * @param query
	 * @return
	 */
	public Set<Integer> getPatientSubsetForQuery(Query query) {
		Set<Integer> patientIdSet = idSetsForEachFilter(query);

        // todo: make sure this can safely be ignored
		/*TreeSet<Integer> idList;
		if(patientIdSet.isEmpty()) {
			if(variantService.getPatientIds().length > 0 ) {
				idList = new TreeSet(
						Sets.union(phenotypeMetaStore.getPatientIds(),
								new TreeSet(Arrays.asList(
										variantService.getPatientIds()).stream()
										.collect(Collectors.mapping(
												(String id)->{return Integer.parseInt(id.trim());}, Collectors.toList()))) ));
			}else {
				idList = phenotypeMetaStore.getPatientIds();
			}
		}else {
			idList = new TreeSet<>(applyBooleanLogic(patientIdSet));
		}*/
		if (patientIdSet == null) {
			return phenotypeMetaStore.getPatientIds();
		}
		return patientIdSet;
	}

	private List<Set<Integer>> getIdSetsForRequiredFields(Query query, DistributableQuery distributableQuery) {
		if(!query.getRequiredFields().isEmpty()) {
            return query.getRequiredFields().stream().map(path -> {
                if (VariantUtils.pathIsVariantSpec(path)) {
                    distributableQuery.addRequiredVariantField(path);
                    return null;
                } else {
                    return new HashSet<Integer>(getCube(path).keyBasedIndex());
                }
            }).filter(Objects::nonNull).collect(Collectors.toList());
        }
        return List.of();
	}

	private Optional<Set<Integer>> getPatientIdsForAnyRecordOf(List<String> anyRecordOfFilters, DistributableQuery distributableQuery) {
		if(!anyRecordOfFilters.isEmpty()) {
			// This is an OR aggregation of anyRecordOf filters
			Set<Integer> anyRecordOfPatientSet = anyRecordOfFilters.parallelStream().flatMap(path -> {
				if (VariantUtils.pathIsVariantSpec(path)) {
					throw new IllegalArgumentException("Variant paths not allowed for anyRecordOf queries");
				}
				try {
					return (Stream<Integer>) getCube(path).keyBasedIndex().stream();
				} catch (Exception e) {
					return Stream.empty();
				}
			}).collect(Collectors.toSet());
			return Optional.of(anyRecordOfPatientSet);
		}
        return Optional.empty();
	}

	private List<Set<Integer>> getIdSetsForNumericFilters(Query query) {
		if(!query.getNumericFilters().isEmpty()) {
			return query.getNumericFilters().entrySet().stream().map(entry -> {
				Set<Integer> keysForRange = getCube(entry.getKey()).getKeysForRange(entry.getValue().getMin(), entry.getValue().getMax());
				return keysForRange;
			}).collect(Collectors.toList());
		}
        return List.of();
	}

	private List<Set<Integer>> getIdSetsForCategoryFilters(Query query, DistributableQuery distributableQuery) {
		if(!query.getCategoryFilters().isEmpty()) {
			return query.getCategoryFilters().entrySet().stream().map((entry) -> {
				Set<Integer> ids = new TreeSet<>();
				if (VariantUtils.pathIsVariantSpec(entry.getKey())) {
					distributableQuery.addVariantSpecCategoryFilter(entry.getKey(), entry.getValue());
					return null;
				} else {
					for (String category : entry.getValue()) {
						ids.addAll(getCube(entry.getKey()).getKeysForValue(category));
					}
				}
				return ids;
			}).filter(Objects::nonNull).collect(Collectors.toList());
		}
        return List.of();
	}

	public Collection<String> getVariantList(Query query) {
		DistributableQuery distributableQuery = getDistributableQuery(query);
		return genomicProcessor.getVariantList(distributableQuery).block();
	}

	public List<InfoColumnMeta> getInfoStoreMeta() {
		return genomicProcessor.getInfoColumnMeta();
	}

	public List<String> searchInfoConceptValues(String conceptPath, String query) {
		try {
			return genomicProcessor.getInfoStoreValues(conceptPath).stream()
					.filter(variableValue -> variableValue.toUpperCase(Locale.ENGLISH).contains(query.toUpperCase(Locale.ENGLISH)))
					.collect(Collectors.toList());
		} catch (UncheckedExecutionException e) {
			if(e.getCause() instanceof RuntimeException) {
				throw (RuntimeException) e.getCause();
			}
			throw e;
		}
	}

	//
	//	private boolean pathIsGeneName(String key) {
	//		return new GeneLibrary().geneNameSearch(key).size()==1;
	//	}

	/**
	 * If there are concepts in the list of paths which are already in the cache, push those to the
	 * front of the list so that we don't evict and then reload them for concepts which are not yet
	 * in the cache.
	 *
	 * @param paths
	 * @param columnCount
	 * @return
	 */
	protected ArrayList<Integer> useResidentCubesFirst(List<String> paths, int columnCount) {
		int x;
		TreeSet<String> pathSet = new TreeSet<String>(paths);
		Set<String> residentKeys = Sets.intersection(pathSet, store.asMap().keySet());

		ArrayList<Integer> columnIndex = new ArrayList<Integer>();

		residentKeys.forEach(key ->{
			columnIndex.add(paths.indexOf(key) + 1);
		});

		Sets.difference(pathSet, residentKeys).forEach(key->{
			columnIndex.add(paths.indexOf(key) + 1);
		});

		for(x = 1;x < columnCount;x++) {
			columnIndex.add(x);
		}
		return columnIndex;
	}

	/**
	 * Load the variantStore object from disk and build the PhenoCube cache.
	 *
	 * @return
	 */
	protected LoadingCache<String, PhenoCube<?>> initializeCache() {
		return CacheBuilder.newBuilder()
				.maximumSize(CACHE_SIZE)
				.build(
						new CacheLoader<String, PhenoCube<?>>() {
							public PhenoCube<?> load(String key) throws Exception {
								try(RandomAccessFile allObservationsStore = new RandomAccessFile(hpdsDataDirectory + "allObservationsStore.javabin", "r");){
									ColumnMeta columnMeta = phenotypeMetaStore.getColumnMeta(key);
									if(columnMeta != null) {
										allObservationsStore.seek(columnMeta.getAllObservationsOffset());
										int length = (int) (columnMeta.getAllObservationsLength() - columnMeta.getAllObservationsOffset());
										byte[] buffer = new byte[length];
										allObservationsStore.read(buffer);
										allObservationsStore.close();
										try (ObjectInputStream inStream = new ObjectInputStream(new ByteArrayInputStream(Crypto.decryptData(buffer)))) {
											return (PhenoCube<?>)inStream.readObject();
										}
									}else {
										log.warn("ColumnMeta not found for : [{}]", key);
										return null;
									}
								}
							}
						});
	}


	protected PhenoCube getCube(String path) {
		try {
			return store.get(path);
		} catch (ExecutionException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Get a cube without throwing an error if not found.
	 * Useful for federated pic-sure's where there are fewer
	 * guarantees about concept paths.
	 */
	public Optional<PhenoCube<?>> nullableGetCube(String path) {
		try {
			return Optional.ofNullable(store.get(path));
		} catch (InvalidCacheLoadException | ExecutionException e) {
			return Optional.empty();
		}
	}

	public TreeMap<String, ColumnMeta> getDictionary() {
		return phenotypeMetaStore.getMetaStore();
	}

	public List<String> getPatientIds() {
		return genomicProcessor.getPatientIds();
	}

	public Optional<VariableVariantMasks> getMasks(String path, VariantBucketHolder<VariableVariantMasks> variantMasksVariantBucketHolder) {
		return genomicProcessor.getMasks(path, variantMasksVariantBucketHolder);
	}

    // todo: handle this locally, we do not want this in the genomic processor
    protected VariantMask createMaskForPatientSet(Set<Integer> patientSubset) {
        return genomicProcessor.createMaskForPatientSet(patientSubset);
    }
}

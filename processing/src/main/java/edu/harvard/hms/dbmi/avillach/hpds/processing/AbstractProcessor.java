package edu.harvard.hms.dbmi.avillach.hpds.processing;

import java.io.*;
import java.math.BigInteger;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.google.errorprone.annotations.Var;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.*;
import com.google.common.cache.CacheLoader.InvalidCacheLoadException;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;

import edu.harvard.hms.dbmi.avillach.hpds.crypto.Crypto;
import edu.harvard.hms.dbmi.avillach.hpds.data.genotype.*;
import edu.harvard.hms.dbmi.avillach.hpds.data.genotype.caching.VariantBucketHolder;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.ColumnMeta;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.PhenoCube;
import edu.harvard.hms.dbmi.avillach.hpds.data.query.Filter.FloatFilter;
import edu.harvard.hms.dbmi.avillach.hpds.data.query.Query;
import edu.harvard.hms.dbmi.avillach.hpds.data.query.Query.VariantInfoFilter;
import edu.harvard.hms.dbmi.avillach.hpds.storage.FileBackedByteIndexedStorage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.persistence.Column;

// todo: rename this class. VariantService maybe?
@Component
public class AbstractProcessor {

	private static Logger log = LoggerFactory.getLogger(AbstractProcessor.class);

	private static final double MAX_SPARSE_INDEX_RATIO = 0.1;

	private BucketIndexBySample bucketIndex;
	private final Integer VARIANT_INDEX_BLOCK_SIZE = 1000000;
	private final String VARIANT_INDEX_FBBIS_STORAGE_FILE = "/opt/local/hpds/all/variantIndex_fbbis_storage.javabin";
	private final String VARIANT_INDEX_FBBIS_FILE = "/opt/local/hpds/all/variantIndex_fbbis.javabin";
	private final String BUCKET_INDEX_BY_SAMPLE_FILE = "/opt/local/hpds/all/BucketIndexBySample.javabin";

	private final String HOMOZYGOUS_VARIANT = "1/1";
	private final String HETEROZYGOUS_VARIANT = "0/1";
	private final String HOMOZYGOUS_REFERENCE = "0/0";

	private final String ID_CUBE_NAME;
	private final int ID_BATCH_SIZE;
	private final int CACHE_SIZE;



	private List<String> infoStoreColumns;

	private Map<String, FileBackedByteIndexedInfoStore> infoStores;

	private LoadingCache<String, PhenoCube<?>> store;

	//variantStore will never be null; it is initialized to an empty object.
	private final VariantStore variantStore;

	private final PhenotypeMetaStore phenotypeMetaStore;

	@Autowired
	public AbstractProcessor(PhenotypeMetaStore phenotypeMetaStore) throws ClassNotFoundException, IOException {
		this.phenotypeMetaStore = phenotypeMetaStore;

		CACHE_SIZE = Integer.parseInt(System.getProperty("CACHE_SIZE", "100"));
		ID_BATCH_SIZE = Integer.parseInt(System.getProperty("ID_BATCH_SIZE", "0"));
		ID_CUBE_NAME = System.getProperty("ID_CUBE_NAME", "NONE");

		variantStore = VariantStore.deserializeInstance();

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
		infoStores = new HashMap<>();
		File genomicDataDirectory = new File("/opt/local/hpds/all/");
		if(genomicDataDirectory.exists() && genomicDataDirectory.isDirectory()) {
			Arrays.stream(genomicDataDirectory.list((file, filename)->{return filename.endsWith("infoStore.javabin");}))
					.forEach((String filename)->{
						try (
								FileInputStream fis = new FileInputStream("/opt/local/hpds/all/" + filename);
								GZIPInputStream gis = new GZIPInputStream(fis);
								ObjectInputStream ois = new ObjectInputStream(gis)
						){
							log.info("loading " + filename);
							FileBackedByteIndexedInfoStore infoStore = (FileBackedByteIndexedInfoStore) ois.readObject();
							infoStores.put(filename.replace("_infoStore.javabin", ""), infoStore);
							ois.close();
						} catch (FileNotFoundException e) {
							e.printStackTrace();
						} catch (IOException e) {
							e.printStackTrace();
						} catch (ClassNotFoundException e) {
							e.printStackTrace();
						}
					});
		}
		try {
			loadGenomicCacheFiles();
		} catch (Throwable e) {
			log.error("Failed to load genomic data: " + e.getLocalizedMessage(), e);
		}
		infoStoreColumns = new ArrayList<String>(infoStores.keySet());
		warmCaches();
	}

	public AbstractProcessor(PhenotypeMetaStore phenotypeMetaStore, LoadingCache<String,
			PhenoCube<?>> store, Map<String, FileBackedByteIndexedInfoStore> infoStores, List<String> infoStoreColumns, VariantStore variantStore) {
		this.phenotypeMetaStore = phenotypeMetaStore;
		this.store = store;
		this.infoStores = infoStores;
		this.infoStoreColumns = infoStoreColumns;
		this.variantStore = variantStore;

		CACHE_SIZE = Integer.parseInt(System.getProperty("CACHE_SIZE", "100"));
		ID_BATCH_SIZE = Integer.parseInt(System.getProperty("ID_BATCH_SIZE", "0"));
		ID_CUBE_NAME = System.getProperty("ID_CUBE_NAME", "NONE");
	}

	public VariantStore getVariantStore() {
		return variantStore;
	}

	public List<String> getInfoStoreColumns() {
		return infoStoreColumns;
	}

	private void warmCaches() {
		//infoCache.refresh("Variant_frequency_as_text_____Rare");
		//infoCache.refresh("Variant_frequency_as_text_____Common");
		//infoCache.refresh("Variant_frequency_as_text_____Novel");
	}


	/**
	 * This process takes a while (even after the cache is built), so let's spin it out into it's own thread. (not done yet)
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private synchronized void loadGenomicCacheFiles() throws FileNotFoundException, IOException, InterruptedException {
		//skip if we have no variants
		if(variantStore.getPatientIds().length == 0) {
			variantIndex = new String[0];
			log.warn("No Genomic Data found.  Skipping variant Indexing");
			return;
		}

		if(bucketIndex==null) {
			if(variantIndex==null) {
				if(!new File(VARIANT_INDEX_FBBIS_FILE).exists()) {
					log.info("Creating new " + VARIANT_INDEX_FBBIS_FILE);
					populateVariantIndex();
					FileBackedByteIndexedStorage<Integer, String[]> fbbis =
							new FileBackedByteIndexedStorage<Integer, String[]>(Integer.class, String[].class, new File(VARIANT_INDEX_FBBIS_STORAGE_FILE));
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
										// TODO Auto-generated catch block
										e.printStackTrace();
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

	/**
	 * Merges a list of sets of patient ids by intersection. If we implemented OR semantics
	 * this would be where the change happens.
	 *
	 * @param filteredIdSets
	 * @return
	 */
	protected Set<Integer> applyBooleanLogic(ArrayList<Set<Integer>> filteredIdSets) {
		Set<Integer>[] ids = new Set[] {filteredIdSets.get(0)};
		filteredIdSets.forEach((keySet)->{
			ids[0] = Sets.intersection(ids[0], keySet);
		});
		return ids[0];
	}

	/**
	 * For each filter in the query, return a set of patient ids that match. The order of these sets in the
	 * returned list of sets does not matter and cannot currently be tied back to the filter that generated
	 * it.
	 *
	 * @param query
	 * @return
	 */
	protected ArrayList<Set<Integer>> idSetsForEachFilter(Query query) {
		ArrayList<Set<Integer>> filteredIdSets = new ArrayList<Set<Integer>>();

		try {
			addIdSetsForAnyRecordOf(query, filteredIdSets);
			addIdSetsForRequiredFields(query, filteredIdSets);
			addIdSetsForNumericFilters(query, filteredIdSets);
			addIdSetsForCategoryFilters(query, filteredIdSets);
		} catch (InvalidCacheLoadException e) {
			log.warn("Invalid query supplied: " + e.getLocalizedMessage());
			filteredIdSets.add(new HashSet<Integer>()); // if an invalid path is supplied, no patients should match.
		}

		//AND logic to make sure all patients match each filter
		if(filteredIdSets.size()>1) {
			filteredIdSets = new ArrayList<Set<Integer>>(List.of(applyBooleanLogic(filteredIdSets)));
		}

		addIdSetsForVariantInfoFilters(query, filteredIdSets);

		return filteredIdSets;
	}

	/**
	 * Process each filter in the query and return a list of patient ids that should be included in the
	 * result.
	 *
	 * @param query
	 * @return
	 */
	protected TreeSet<Integer> getPatientSubsetForQuery(Query query) {
		ArrayList<Set<Integer>> filteredIdSets;

		filteredIdSets = idSetsForEachFilter(query);

		TreeSet<Integer> idList;
		if(filteredIdSets.isEmpty()) {
			if(variantStore.getPatientIds().length > 0 ) {
				idList = new TreeSet(
						Sets.union(phenotypeMetaStore.getPatientIds(),
								new TreeSet(Arrays.asList(
										variantStore.getPatientIds()).stream()
										.collect(Collectors.mapping(
												(String id)->{return Integer.parseInt(id.trim());}, Collectors.toList()))) ));
			}else {
				idList = phenotypeMetaStore.getPatientIds();
			}
		}else {
			idList = new TreeSet<Integer>(applyBooleanLogic(filteredIdSets));
		}
		return idList;
	}

	private void addIdSetsForRequiredFields(Query query, ArrayList<Set<Integer>> filteredIdSets) {
		if(!query.getRequiredFields().isEmpty()) {
			VariantBucketHolder<VariantMasks> bucketCache = new VariantBucketHolder<>();
			filteredIdSets.addAll((Set<TreeSet<Integer>>)(query.getRequiredFields().parallelStream().map(path->{
				if(VariantUtils.pathIsVariantSpec(path)) {
					TreeSet<Integer> patientsInScope = new TreeSet<Integer>();
					addIdSetsForVariantSpecCategoryFilters(new String[]{"0/1","1/1"}, path, patientsInScope, bucketCache);
					return patientsInScope;
				} else {
					return new TreeSet<Integer>(getCube(path).keyBasedIndex());
				}
			}).collect(Collectors.toSet())));
		}
	}

	private void addIdSetsForAnyRecordOf(Query query, ArrayList<Set<Integer>> filteredIdSets) {
		if(!query.getAnyRecordOf().isEmpty()) {
			Set<Integer> patientsInScope = new ConcurrentSkipListSet<Integer>();
			VariantBucketHolder<VariantMasks> bucketCache = new VariantBucketHolder<VariantMasks>();
			query.getAnyRecordOf().parallelStream().forEach(path->{
				if(patientsInScope.size()<Math.max(
						phenotypeMetaStore.getPatientIds().size(),
						variantStore.getPatientIds().length)) {
					if(VariantUtils.pathIsVariantSpec(path)) {
						addIdSetsForVariantSpecCategoryFilters(new String[]{"0/1","1/1"}, path, patientsInScope, bucketCache);
					} else {
						patientsInScope.addAll(getCube(path).keyBasedIndex());
					}
				}
			});
			filteredIdSets.add(patientsInScope);
		}
	}

	private void addIdSetsForNumericFilters(Query query, ArrayList<Set<Integer>> filteredIdSets) {
		if(!query.getNumericFilters().isEmpty()) {
			filteredIdSets.addAll((Set<TreeSet<Integer>>)(query.getNumericFilters().entrySet().parallelStream().map(entry->{
				return (TreeSet<Integer>)(getCube(entry.getKey()).getKeysForRange(entry.getValue().getMin(), entry.getValue().getMax()));
			}).collect(Collectors.toSet())));
		}
	}

	private void addIdSetsForCategoryFilters(Query query, ArrayList<Set<Integer>> filteredIdSets) {
		if(!query.getCategoryFilters().isEmpty()) {
			VariantBucketHolder<VariantMasks> bucketCache = new VariantBucketHolder<VariantMasks>();
			Set<Set<Integer>> idsThatMatchFilters = (Set<Set<Integer>>)query.getCategoryFilters().entrySet().parallelStream().map(entry->{
				Set<Integer> ids = new TreeSet<Integer>();
				if(VariantUtils.pathIsVariantSpec(entry.getKey())) {
					addIdSetsForVariantSpecCategoryFilters(entry.getValue(), entry.getKey(), ids, bucketCache);
				} else {
					String[] categoryFilter = entry.getValue();
					for(String category : categoryFilter) {
							ids.addAll(getCube(entry.getKey()).getKeysForValue(category));
					}
				}
				return ids;
			}).collect(Collectors.toSet());
			filteredIdSets.addAll(idsThatMatchFilters);
		}
	}

	private void addIdSetsForVariantSpecCategoryFilters(String[] zygosities, String key, Set<Integer> ids, VariantBucketHolder<VariantMasks> bucketCache) {
		ArrayList<BigInteger> variantBitmasks = getBitmasksForVariantSpecCategoryFilter(zygosities, key, bucketCache);
		if( ! variantBitmasks.isEmpty()) {
			BigInteger bitmask = variantBitmasks.get(0);
			if(variantBitmasks.size()>1) {
				for(int x = 1;x<variantBitmasks.size();x++) {
					bitmask = bitmask.or(variantBitmasks.get(x));
				}
			}
			String bitmaskString = bitmask.toString(2);
			log.debug("or'd masks : " + bitmaskString);
			// TODO : This is much less efficient than using bitmask.testBit(x)
			for(int x = 2;x < bitmaskString.length()-2;x++) {
				if('1'==bitmaskString.charAt(x)) {
					String patientId = variantStore.getPatientIds()[x-2];
					try{
						ids.add(Integer.parseInt(patientId));
					}catch(NullPointerException | NoSuchElementException e) {
						log.error(ID_CUBE_NAME + " has no value for patientId : " + patientId);
					}
				}
			}
		}
	}

	private ArrayList<BigInteger> getBitmasksForVariantSpecCategoryFilter(String[] zygosities, String variantName, VariantBucketHolder<VariantMasks> bucketCache) {
		ArrayList<BigInteger> variantBitmasks = new ArrayList<>();
		variantName = variantName.replaceAll(",\\d/\\d$", "");
		log.debug("looking up mask for : " + variantName);
		VariantMasks masks;
		try {
			masks = variantStore.getMasks(variantName, bucketCache);
			Arrays.stream(zygosities).forEach((zygosity) -> {
				if(masks!=null) {
					if(zygosity.equals(HOMOZYGOUS_REFERENCE)) {
						BigInteger homozygousReferenceBitmask = calculateIndiscriminateBitmask(masks);
						for(int x = 2;x<homozygousReferenceBitmask.bitLength()-2;x++) {
							homozygousReferenceBitmask = homozygousReferenceBitmask.flipBit(x);
						}
						variantBitmasks.add(homozygousReferenceBitmask);
					} else if(masks.heterozygousMask != null && zygosity.equals(HETEROZYGOUS_VARIANT)) {
						variantBitmasks.add(masks.heterozygousMask);
					}else if(masks.homozygousMask != null && zygosity.equals(HOMOZYGOUS_VARIANT)) {
						variantBitmasks.add(masks.homozygousMask);
					}else if(zygosity.equals("")) {
						variantBitmasks.add(calculateIndiscriminateBitmask(masks));
					}
				} else {
					variantBitmasks.add(variantStore.emptyBitmask());
				}

			});
		} catch (IOException e) {
			e.printStackTrace();
		}
		return variantBitmasks;
	}

	/**
	 * Calculate a bitmask which is a bitwise OR of any populated masks in the VariantMasks passed in
	 * @param masks
	 * @return
	 */
	private BigInteger calculateIndiscriminateBitmask(VariantMasks masks) {
		BigInteger indiscriminateVariantBitmask = null;
		if(masks.heterozygousMask == null && masks.homozygousMask != null) {
			indiscriminateVariantBitmask = masks.homozygousMask;
		}else if(masks.homozygousMask == null && masks.heterozygousMask != null) {
			indiscriminateVariantBitmask = masks.heterozygousMask;
		}else if(masks.homozygousMask != null && masks.heterozygousMask != null) {
			indiscriminateVariantBitmask = masks.heterozygousMask.or(masks.homozygousMask);
		}else {
			indiscriminateVariantBitmask = variantStore.emptyBitmask();
		}
		return indiscriminateVariantBitmask;
	}

	protected void addIdSetsForVariantInfoFilters(Query query, ArrayList<Set<Integer>> filteredIdSets) {
//		log.debug("filterdIDSets START size: " + filteredIdSets.size());
		/* VARIANT INFO FILTER HANDLING IS MESSY */
		if(!query.getVariantInfoFilters().isEmpty()) {
			for(VariantInfoFilter filter : query.getVariantInfoFilters()){
				ArrayList<VariantIndex> variantSets = new ArrayList<>();
				addVariantsMatchingFilters(filter, variantSets);
				log.info("Found " + variantSets.size() + " groups of sets for patient identification");
				//log.info("found " + variantSets.stream().mapToInt(Set::size).sum() + " variants for identification");
				if(!variantSets.isEmpty()) {
					// INTERSECT all the variant sets.
					VariantIndex intersectionOfInfoFilters = variantSets.get(0);
					for(VariantIndex variantSet : variantSets) {
						intersectionOfInfoFilters = intersectionOfInfoFilters.intersection(variantSet);
					}
					// Apparently set.size() is really expensive with large sets... I just saw it take 17 seconds for a set with 16.7M entries
					if(log.isDebugEnabled()) {
						//IntSummaryStatistics stats = variantSets.stream().collect(Collectors.summarizingInt(set->set.size()));
						//log.debug("Number of matching variants for all sets : " + stats.getSum());
						//log.debug("Number of matching variants for intersection of sets : " + intersectionOfInfoFilters.size());
					}
					// add filteredIdSet for patients who have matching variants, heterozygous or homozygous for now.
					addPatientIdsForIntersectionOfVariantSets(filteredIdSets, intersectionOfInfoFilters);
				}
			}
		}
		/* END OF VARIANT INFO FILTER HANDLING */
	}

	Weigher<String, VariantIndex> weigher = new Weigher<String, VariantIndex>(){
		@Override
		public int weigh(String key, VariantIndex value) {
			if (value instanceof DenseVariantIndex) {
				return ((DenseVariantIndex) value).getVariantIndexMask().length;
			} else if (value instanceof SparseVariantIndex) {
				return ((SparseVariantIndex) value).getVariantIds().size();
			} else {
				throw new IllegalArgumentException("Unknown VariantIndex implementation: " + value.getClass());
			}
		}
	};

	private void populateVariantIndex() throws InterruptedException {
		int[] numVariants = {0};
		HashMap<String, String[]> contigMap = new HashMap<>();

		ExecutorService ex = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		variantStore.variantMaskStorage.entrySet().forEach(entry->{
			ex.submit(()->{
				int numVariantsInContig = 0;
				FileBackedByteIndexedStorage<Integer, ConcurrentHashMap<String, VariantMasks>> storage = entry.getValue();
				HashMap<Integer, String[]> bucketMap = new HashMap<>();
				log.info("Creating bucketMap for contig " + entry.getKey());
				for(Integer bucket: storage.keys()){
					try {
						ConcurrentHashMap<String, VariantMasks> bucketStorage = storage.get(bucket);
						numVariantsInContig += bucketStorage.size();
						bucketMap.put(bucket, bucketStorage.keySet().toArray(new String[0]));
					} catch (IOException e) {
						log.error("an error occurred", e);
					}
				};
				log.info("Completed bucketMap for contig " + entry.getKey());
				String[] variantsInContig = new String[numVariantsInContig];
				int current = 0;
				for(String[] bucketList  : bucketMap.values()) {
					System.arraycopy(bucketList, 0, variantsInContig, current, bucketList.length);
					current = current + bucketList.length;
				}
				bucketMap.clear();
				synchronized(numVariants) {
					log.info("Found " + variantsInContig.length + " variants in contig " + entry.getKey() + ".");
					contigMap.put(entry.getKey(), variantsInContig);
					numVariants[0] += numVariantsInContig;
				}
			});
		});
		ex.shutdown();
		while(!ex.awaitTermination(10, TimeUnit.SECONDS)) {
			Thread.sleep(20000);
			log.info("Awaiting completion of variant index");
		}

		log.info("Found " + numVariants[0] + " total variants.");

		variantIndex = new String[numVariants[0]];

		int current = 0;
		for(String[] contigList  : contigMap.values()) {
			System.arraycopy(contigList, 0, variantIndex, current, contigList.length);
			current = current + contigList.length;
		}
		contigMap.clear();

		Arrays.sort(variantIndex);
		log.info("Index created with " + variantIndex.length + " total variants.");

	}

	// why is this not VariantSpec[]?
	protected static String[] variantIndex = null;

	CacheLoader<String, VariantIndex> cacheLoader = new CacheLoader<>() {
		@Override
		public VariantIndex load(String infoColumn_valueKey) throws Exception {
			log.debug("Calculating value for cache for key " + infoColumn_valueKey);
			long time = System.currentTimeMillis();
			String[] column_and_value = infoColumn_valueKey.split(COLUMN_AND_KEY_DELIMITER);
			String[] variantArray = infoStores.get(column_and_value[0]).getAllValues().get(column_and_value[1]);

			if ((double)variantArray.length / (double)variantIndex.length < MAX_SPARSE_INDEX_RATIO ) {
				Set<Integer> variantIds = new HashSet<>();
				for(String variantSpec : variantArray) {
					int variantIndexArrayIndex = Arrays.binarySearch(variantIndex, variantSpec);
					variantIds.add(variantIndexArrayIndex);
				}
				return new SparseVariantIndex(variantIds);
			} else {
				boolean[] variantIndexArray = new boolean[variantIndex.length];
				int x = 0;
				for(String variantSpec : variantArray) {
					int variantIndexArrayIndex = Arrays.binarySearch(variantIndex, variantSpec);
					// todo: shouldn't this be greater than or equal to 0? 0 is a valid index
					if (variantIndexArrayIndex > 0) {
						variantIndexArray[variantIndexArrayIndex] = true;
					}
				}
				log.debug("Cache value for key " + infoColumn_valueKey + " calculated in " + (System.currentTimeMillis() - time) + " ms");
				return new DenseVariantIndex(variantIndexArray);
			}
		}
	};
	LoadingCache<String, VariantIndex> infoCache = CacheBuilder.newBuilder()
			.weigher(weigher).maximumWeight(10000000000L).build(cacheLoader);

	protected void addVariantsMatchingFilters(VariantInfoFilter filter, ArrayList<VariantIndex> variantSets) {
		// Add variant sets for each filter
		if(filter.categoryVariantInfoFilters != null && !filter.categoryVariantInfoFilters.isEmpty()) {
			filter.categoryVariantInfoFilters.entrySet().parallelStream().forEach((Entry<String,String[]> entry) ->{
				addVariantsMatchingCategoryFilter(variantSets, entry);
			});
		}
		if(filter.numericVariantInfoFilters != null && !filter.numericVariantInfoFilters.isEmpty()) {
			filter.numericVariantInfoFilters.forEach((String column, FloatFilter doubleFilter)->{
				FileBackedByteIndexedInfoStore infoStore = getInfoStore(column);

				doubleFilter.getMax();
				Range<Float> filterRange = Range.closed(doubleFilter.getMin(), doubleFilter.getMax());
				List<String> valuesInRange = infoStore.continuousValueIndex.getValuesInRange(filterRange);
				VariantIndex variants = new SparseVariantIndex(Set.of());
				for(String value : valuesInRange) {
					try {
						variants = variants.union(infoCache.get(columnAndKey(column, value)));
					} catch (ExecutionException e) {
						log.error("an error occurred", e);
					}
				}
				variantSets.add(variants);
			});
		}
	}

	private Set<String> arrayToSet(int[] variantSpecs) {
		ConcurrentHashMap<String, String> setMap = new ConcurrentHashMap<String, String>(variantSpecs.length);
		Arrays.stream(variantSpecs).parallel().forEach((index)->{
			String variantSpec = variantIndex[index];
			setMap.put(variantSpec, variantSpec);
		});
		return setMap.keySet();
	}

	private boolean[] unionBooleans(boolean[] array1, boolean[] array2) {
		// todo: validate length
		boolean[] result = new boolean[array1.length];
		for (int i = 0; i < array1.length; i++) {
			result[i] = array1[i] || array2[i];
		}
		return result;
	}

	private boolean[] intersectBooleans(boolean[] array1, boolean[] array2) {
		// todo: validate length
		boolean[] result = new boolean[array1.length];
		for (int i = 0; i < array1.length; i++) {
			result[i] = array1[i] && array2[i];
		}
		return result;
	}

	private void addVariantsMatchingCategoryFilter(ArrayList<VariantIndex> variantSets, Entry<String, String[]> entry) {
		String column = entry.getKey();
		String[] values = entry.getValue();
		Arrays.sort(values);
		FileBackedByteIndexedInfoStore infoStore = getInfoStore(column);

		List<String> infoKeys = filterInfoCategoryKeys(values, infoStore);
		/*
		 * We want to union all the variants for each selected key, so we need an intermediate set
		 */
		VariantIndex[] categoryVariantSets = new VariantIndex[] {new SparseVariantIndex(Set.of())};

		if(infoKeys.size()>1) {
			infoKeys.stream().forEach((key)->{
				try {
					VariantIndex variantsForColumnAndValue = infoCache.get(columnAndKey(column, key));
					categoryVariantSets[0] = categoryVariantSets[0].union(variantsForColumnAndValue);
				} catch (ExecutionException e) {
					log.error("an error occurred", e);
				}
			});
		} else {
			try {
				categoryVariantSets[0] = infoCache.get(columnAndKey(column, infoKeys.get(0)));
			} catch (ExecutionException e) {
				log.error("an error occurred", e);
			}
		}
		variantSets.add(categoryVariantSets[0]);
	}

	private List<String> filterInfoCategoryKeys(String[] values, FileBackedByteIndexedInfoStore infoStore) {
		List<String> infoKeys = infoStore.getAllValues().keys().stream().filter((String key)->{

			// iterate over the values for the specific category and find which ones match the search

			int insertionIndex = Arrays.binarySearch(values, key);
			return insertionIndex > -1 && insertionIndex < values.length;
		}).collect(Collectors.toList());
		log.info("found " + infoKeys.size() + " keys");
		return infoKeys;
	}

	private static final String COLUMN_AND_KEY_DELIMITER = "_____";
	private String columnAndKey(String column, String key) {
		return column + COLUMN_AND_KEY_DELIMITER + key;
	}

	private void addPatientIdsForIntersectionOfVariantSets(ArrayList<Set<Integer>> filteredIdSets,
			VariantIndex intersectionOfInfoFilters) {
		if(/*!intersectionOfInfoFilters.isEmpty()*/true) {
			Set<Integer> patientsInScope;
			Set<Integer> patientIds = Arrays.asList(
					variantStore.getPatientIds()).stream().map((String id)->{
						return Integer.parseInt(id);}).collect(Collectors.toSet());
			if(!filteredIdSets.isEmpty()) {
				// shouldn't we intersect all of these?
				patientsInScope = Sets.intersection(patientIds, filteredIdSets.get(0));
			} else {
				patientsInScope = patientIds;
			}

			BigInteger[] matchingPatients = new BigInteger[] {variantStore.emptyBitmask()};

			Set<String> variantsInScope = intersectionOfInfoFilters.mapToVariantSpec(variantIndex);

			Collection<List<String>> values = variantsInScope.stream()
					.collect(Collectors.groupingByConcurrent((variantSpec) -> {
						return new VariantSpec(variantSpec).metadata.offset / 1000;
					})).values();
			ArrayList<List<String>> variantBucketsInScope = new ArrayList<List<String>>(values);

			log.info("found " + variantBucketsInScope.size() + " buckets");

			//don't error on small result sets (make sure we have at least one element in each partition)
			int partitionSize = variantBucketsInScope.size() / Runtime.getRuntime().availableProcessors();
			List<List<List<String>>> variantBucketPartitions = Lists.partition(variantBucketsInScope, partitionSize > 0 ? partitionSize : 1);

			log.info("and partitioned those into " + variantBucketPartitions.size() + " groups");

			int patientsInScopeSize = patientsInScope.size();
			BigInteger patientsInScopeMask = createMaskForPatientSet(patientsInScope);
			for(int x = 0;
					x < variantBucketPartitions.size() && matchingPatients[0].bitCount() < patientsInScopeSize + 4;
					x++) {
				List<List<String>> variantBuckets = variantBucketPartitions.get(x);
				variantBuckets.parallelStream().forEach((variantBucket)->{
					VariantBucketHolder<VariantMasks> bucketCache = new VariantBucketHolder<VariantMasks>();
					variantBucket.stream().forEach((variantSpec)->{
						VariantMasks masks;
						BigInteger heteroMask = variantStore.emptyBitmask();
						BigInteger homoMask = variantStore.emptyBitmask();
						try {
							masks = variantStore.getMasks(variantSpec, bucketCache);
							if(masks != null) {
//								if(log.isDebugEnabled()) {
//									log.debug("checking variant " + variantSpec + " for patients: " + ( masks.heterozygousMask == null ? "null" :(masks.heterozygousMask.bitCount() - 4))
//											+ "/" + (masks.homozygousMask == null ? "null" : (masks.homozygousMask.bitCount() - 4)) + "    "
//											+ ( masks.heterozygousNoCallMask == null ? "null" :(masks.heterozygousNoCallMask.bitCount() - 4))
//											+ "/" + (masks.homozygousNoCallMask == null ? "null" : (masks.homozygousNoCallMask.bitCount() - 4)));
//								}

								heteroMask = masks.heterozygousMask == null ? variantStore.emptyBitmask() : masks.heterozygousMask;
								homoMask = masks.homozygousMask == null ? variantStore.emptyBitmask() : masks.homozygousMask;
								BigInteger orMasks = heteroMask.or(homoMask);
								BigInteger andMasks = orMasks.and(patientsInScopeMask);
								synchronized(matchingPatients) {
									matchingPatients[0] = matchingPatients[0].or(andMasks);
								}
							}
						} catch (IOException e) {
							log.error("an error occurred", e);
						}
					});
				});
			}
			Set<Integer> ids = new TreeSet<Integer>();
			String bitmaskString = matchingPatients[0].toString(2);
//			log.debug("or'd masks : " + bitmaskString);
			for(int x = 2;x < bitmaskString.length()-2;x++) {
				if('1'==bitmaskString.charAt(x)) {
					String patientId = variantStore.getPatientIds()[x-2].trim();
					ids.add(Integer.parseInt(patientId));
				}
			}
			filteredIdSets.add(ids);

		}else {
			log.error("No matches found for info filters.");
			filteredIdSets.add(new TreeSet<>());
		}
	}

	protected Collection<String> getVariantList(Query query) throws IOException {
		return processVariantList(query);
	}

	private Collection<String> processVariantList(Query query) throws IOException {
		boolean queryContainsVariantInfoFilters = query.getVariantInfoFilters().stream().anyMatch(variantInfoFilter ->
				!variantInfoFilter.categoryVariantInfoFilters.isEmpty() || !variantInfoFilter.numericVariantInfoFilters.isEmpty()
		);
		if(queryContainsVariantInfoFilters) {
			VariantIndex unionOfInfoFilters = new SparseVariantIndex(Set.of());

			// todo: are these not the same thing?
			if(query.getVariantInfoFilters().size()>1) {
				for(VariantInfoFilter filter : query.getVariantInfoFilters()){
					unionOfInfoFilters = addVariantsForInfoFilter(unionOfInfoFilters, filter);
					//log.info("filter " + filter + "  sets: " + Arrays.deepToString(unionOfInfoFilters.toArray()));
				}
			} else {
				unionOfInfoFilters = addVariantsForInfoFilter(unionOfInfoFilters, query.getVariantInfoFilters().get(0));
			}

			Set<Integer> patientSubset = Sets.intersection(getPatientSubsetForQuery(query),
					new HashSet<Integer>(
							Arrays.stream(variantStore.getPatientIds())
							.map((id)->{return Integer.parseInt(id.trim());})
							.collect(Collectors.toList())));
//			log.debug("Patient subset " + Arrays.deepToString(patientSubset.toArray()));

			// If we have all patients then no variants would be filtered, so no need to do further processing
			if(patientSubset.size()==variantStore.getPatientIds().length) {
				log.info("query selects all patient IDs, returning....");
				return unionOfInfoFilters.mapToVariantSpec(variantIndex);
				//return unionOfInfoFilters.parallelStream().map(index -> variantIndex[index]).collect(Collectors.toList());
			}

			BigInteger patientMasks = createMaskForPatientSet(patientSubset);

			Set<String> unionOfInfoFiltersVariantSpecs = unionOfInfoFilters.mapToVariantSpec(variantIndex);
			//Set<String> unionOfInfoFiltersVariantSpecs = unionOfInfoFilters.parallelStream().map(index -> variantIndex[index]).collect(Collectors.toSet());
			Collection<String> variantsInScope = bucketIndex.filterVariantSetForPatientSet(unionOfInfoFiltersVariantSpecs, new ArrayList<>(patientSubset));

			//NC - this is the original variant filtering, which checks the patient mask from each variant against the patient mask from the query
			if(variantsInScope.size()<100000) {
				ConcurrentSkipListSet<String> variantsWithPatients = new ConcurrentSkipListSet<String>();
				variantsInScope.parallelStream().forEach((String variantKey)->{
					VariantMasks masks;
					try {
						masks = variantStore.getMasks(variantKey, new VariantBucketHolder<VariantMasks>());
						if ( masks.heterozygousMask != null && masks.heterozygousMask.and(patientMasks).bitCount()>4) {
							variantsWithPatients.add(variantKey);
						} else if ( masks.homozygousMask != null && masks.homozygousMask.and(patientMasks).bitCount()>4) {
							variantsWithPatients.add(variantKey);
						} else if ( masks.heterozygousNoCallMask != null && masks.heterozygousNoCallMask.and(patientMasks).bitCount()>4) {
							//so heterozygous no calls we want, homozygous no calls we don't
							variantsWithPatients.add(variantKey);
						}
					} catch (IOException e) {
						log.error("an error occurred", e);
					}
				});
				return variantsWithPatients;
			}else {
				return unionOfInfoFiltersVariantSpecs;
			}
		}
		return new ArrayList<>();
	}

	private VariantIndex addVariantsForInfoFilter(VariantIndex unionOfInfoFilters, VariantInfoFilter filter) {
		ArrayList<VariantIndex> variantSets = new ArrayList<>();
		addVariantsMatchingFilters(filter, variantSets);

		if(!variantSets.isEmpty()) {
			VariantIndex intersectionOfInfoFilters = variantSets.get(0);
			for(VariantIndex variantSet : variantSets) {
				//						log.info("Variant Set : " + Arrays.deepToString(variantSet.toArray()));
				intersectionOfInfoFilters = intersectionOfInfoFilters.intersection(variantSet);
			}
			unionOfInfoFilters = unionOfInfoFilters.union(intersectionOfInfoFilters);
		} else {
			log.warn("No info filters included in query.");
		}
		return unionOfInfoFilters;
	}

	protected BigInteger createMaskForPatientSet(Set<Integer> patientSubset) {
		StringBuilder builder = new StringBuilder("11"); //variant bitmasks are bookended with '11'
		for(String patientId : variantStore.getPatientIds()) {
			Integer idInt = Integer.parseInt(patientId);
			if(patientSubset.contains(idInt)){
				builder.append("1");
			} else {
				builder.append("0");
			}
		}
		builder.append("11"); // masks are bookended with '11' set this so we don't count those

//		log.debug("PATIENT MASK: " + builder.toString());

		BigInteger patientMasks = new BigInteger(builder.toString(), 2);
		return patientMasks;
	}

	public FileBackedByteIndexedInfoStore getInfoStore(String column) {
		return infoStores.get(column);
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
	 * @throws ClassNotFoundException
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	protected LoadingCache<String, PhenoCube<?>> initializeCache() throws ClassNotFoundException, FileNotFoundException, IOException {
		return CacheBuilder.newBuilder()
				.maximumSize(CACHE_SIZE)
				.build(
						new CacheLoader<String, PhenoCube<?>>() {
							public PhenoCube<?> load(String key) throws Exception {
								try(RandomAccessFile allObservationsStore = new RandomAccessFile("/opt/local/hpds/allObservationsStore.javabin", "r");){
									ColumnMeta columnMeta = phenotypeMetaStore.getColumnMeta(key);
									if(columnMeta != null) {
										allObservationsStore.seek(columnMeta.getAllObservationsOffset());
										int length = (int) (columnMeta.getAllObservationsLength() - columnMeta.getAllObservationsOffset());
										byte[] buffer = new byte[length];
										allObservationsStore.read(buffer);
										allObservationsStore.close();
										ObjectInputStream inStream = new ObjectInputStream(new ByteArrayInputStream(Crypto.decryptData(buffer)));
										PhenoCube<?> ret = (PhenoCube<?>)inStream.readObject();
										inStream.close();
										return ret;
									}else {
										System.out.println("ColumnMeta not found for : [" + key + "]");
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

	public TreeMap<String, ColumnMeta> getDictionary() {
		return phenotypeMetaStore.getMetaStore();
	}

	/**
	 * Converts a set of variant IDs to a set of String representations of variant spec. This implementation looks
	 * wonky, but performs much better than other more obvious approaches (ex: Collectors.toSet()) on large sets.
	 */
	public static Set<String> variantIdSetToVariantSpecSet(Set<Integer> variantIds) {
		ConcurrentHashMap<String, String> setMap = new ConcurrentHashMap<>(variantIds.size());
		variantIds.stream().parallel().forEach(index-> setMap.put(variantIndex[index], ""));
		return setMap.keySet();
	}
	public static Set<String> variantIdSetToVariantSpecSet(boolean[] variantIds) {
		ConcurrentHashMap<String, String> setMap = new ConcurrentHashMap<>();
		for (int i = 0; i < variantIds.length; i++) {
			if (variantIds[i]) {
				setMap.put(variantIndex[i], "");
			}
		}
		return setMap.keySet();
	}
}

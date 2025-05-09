package edu.harvard.hms.dbmi.avillach.hpds.etl.genotype;

import java.io.*;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

import edu.harvard.hms.dbmi.avillach.hpds.data.genotype.*;
import edu.harvard.hms.dbmi.avillach.hpds.data.storage.FileBackedStorageVariantMasksImpl;
import edu.harvard.hms.dbmi.avillach.hpds.storage.FileBackedJsonIndexStorage;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.harvard.hms.dbmi.avillach.hpds.storage.FileBackedByteIndexedStorage;
import htsjdk.samtools.util.BlockCompressedInputStream;

public class NewVCFLoader {

	public static final String DEFAULT_VCF_INDEX_FILE = "/opt/local/hpds/vcfIndex.tsv";
	public static final String DEFAULT_STORAGE_DIR = "/opt/local/hpds/all";
	public static final String DEFAULT_MERGED_DIR = "/opt/local/hpds/merged";
	private static Logger logger = LoggerFactory.getLogger(NewVCFLoader.class);

	protected File indexFile;
	protected File storageDir;
	protected String storageDirStr;
	protected String mergedDirStr;

	protected VariantIndexBuilder variantIndexBuilder;
	
	// DO NOT CHANGE THIS unless you want to reload all the data everywhere.
	protected static int CHUNK_SIZE = 1000;

	/**
	 * @param args - if testing, this should be an array ['vcfIndexFile path', 'output storage dir', 'merged dir'].  
	 * by default this will be [/opt/local/hpds/vcfIndex.tsv,  "/opt/local/hpds/all", "/opt/local/hpds/merged" ]. 
	 */
	public static void main(String[] args) throws IOException {

		NewVCFLoader vcfLoader;
		if(args != null && args.length >= 3) {
			logger.info("Reading parameters from input");
			vcfLoader = new NewVCFLoader(new File(args[0]), args[1], args[2]);
		} else {
			logger.info(args.length + " arguments provided");
			logger.info("Using default values");
			vcfLoader = new NewVCFLoader();
		}
		vcfLoader.loadAndMerge();
	}

	protected void loadAndMerge() throws IOException {
		createWalkers();
		loadVCFs();
	}

	public NewVCFLoader() {
		this.indexFile = new File(DEFAULT_VCF_INDEX_FILE);
		this.storageDirStr = DEFAULT_STORAGE_DIR;
		this.storageDir = new File(DEFAULT_STORAGE_DIR);
		this.mergedDirStr = DEFAULT_MERGED_DIR;
		this.variantIndexBuilder = new VariantIndexBuilder();
	}

	public NewVCFLoader(File indexFile, String storageDir, String mergedDirStr) {
		this.indexFile = indexFile;
		this.storageDirStr = storageDir;
		this.storageDir = new File(storageDir);
		this.mergedDirStr = mergedDirStr;
		this.variantIndexBuilder = new VariantIndexBuilder();
	}

	protected ExecutorService chunkWriteEx = Executors.newFixedThreadPool(1);

	protected ConcurrentHashMap<String, InfoStore> infoStoreMap = new ConcurrentHashMap<String, InfoStore>();

	protected HashMap<String, char[][]> zygosityMaskStrings;

	protected TreeMap<String, FileBackedJsonIndexStorage<Integer, ConcurrentHashMap<String, VariableVariantMasks>>> variantMaskStorage = new TreeMap<>();

	protected long startTime;

	protected List<VCFWalker> walkers = new ArrayList<>();

	private boolean contigIsHemizygous;

	protected void loadVCFs() throws IOException {
		startTime = System.currentTimeMillis();
		TreeSet<Integer> allPatientIds = new TreeSet<>();

		// Pull the INFO columns out of the headers for each walker and add all patient ids
		walkers.stream().forEach(walker -> {
			try {
				logger.info("Reading headers of VCF [" + walker.vcfIndexLine.vcfPath + "]");
				walker.readHeaders(infoStoreMap);
				allPatientIds.addAll(Arrays.asList(walker.vcfIndexLine.patientIds));
			} catch (IOException e) {
				logger.error("Error while reading headers of VCF [" + walker.vcfIndexLine.vcfPath + "]", e);
				System.exit(-1);
			}
		});

		Integer[] patientIds = allPatientIds.toArray(new Integer[0]);
		String[] allSampleIds = new String[allPatientIds.size()];

		walkers.parallelStream().forEach(walker -> {
			logger.info("Setting bitmask offsets for VCF [" + walker.vcfIndexLine.vcfPath + "]");
			walker.setBitmaskOffsets(patientIds);
			for (int x = 0; x < walker.vcfIndexLine.sampleIds.length; x++) {
				allSampleIds[Arrays.binarySearch(patientIds,
						walker.vcfIndexLine.patientIds[x])] = walker.vcfIndexLine.sampleIds[x];
			}
		});

		VariantStore store = new VariantStore();
		store.setPatientIds(allPatientIds.stream().map((id) -> {
			return id.toString();
		}).collect(Collectors.toList()).toArray(new String[0]));

		String lastContigProcessed = null;
		int lastChunkProcessed = 0;
		int currentChunk = 0;
		String[] currentContig = new String[1];
		int[] currentPosition = { -1 };
		String[] currentRef = new String[1];
		String[] currentAlt = new String[1];

		zygosityMaskStrings = new HashMap<String/* variantSpec */, char[][]/* string bitmasks */>();

		List<Integer> positionsProcessedInChunk = new ArrayList<>();
		while (walkers.parallelStream().anyMatch(walker -> {
			return walker.hasNext;
		})) {
			Collections.sort(walkers);
			VCFWalker lowestWalker = walkers.get(0);
			String currentSpecNotation = lowestWalker.currentSpecNotation();
			currentContig[0] = lowestWalker.currentContig;
			currentPosition[0] = lowestWalker.currentPosition;
			currentRef[0] = lowestWalker.currentRef;
			currentAlt[0] = lowestWalker.currentAlt;
			currentChunk = lowestWalker.currentPosition / CHUNK_SIZE;
			positionsProcessedInChunk.add(currentPosition[0]);

			if (lastContigProcessed == null) {
				lastContigProcessed = lowestWalker.currentContig;
			}

			flipChunk(lastContigProcessed, lastChunkProcessed, currentChunk, currentContig[0], false,
					lowestWalker.currentLine);
			lastContigProcessed = lowestWalker.currentContig;
			lastChunkProcessed = currentChunk;

			char[][][] maskStringsForVariantSpec = { zygosityMaskStrings.get(currentSpecNotation) };
			if (maskStringsForVariantSpec[0] == null) {
				maskStringsForVariantSpec[0] = new char[7][allPatientIds.size()];
				for (int x = 0; x < maskStringsForVariantSpec[0].length; x++) {
					maskStringsForVariantSpec[0][x] = new char[allPatientIds.size()];
					for (int y = 0; y < allPatientIds.size(); y++) {
						maskStringsForVariantSpec[0][x][y] = '0';
					}
				}
			}
			walkers.stream().filter((walker) -> {
				return walker.currentPosition == currentPosition[0] && walker.currentAlt == currentAlt[0]
						&& walker.currentRef == currentRef[0] && walker.currentContig.contentEquals(currentContig[0]);
			}).forEach((walker) -> {
				walker.updateRecords(maskStringsForVariantSpec[0], infoStoreMap);
				try {
					walker.nextLine();
				} catch (IOException e) {
					throw new UncheckedIOException(e);
				}
			});
			zygosityMaskStrings.put(currentSpecNotation, maskStringsForVariantSpec[0]);
			walkers = walkers.parallelStream().filter((walker) -> {
				return walker.hasNext;
			}).collect(Collectors.toList());
		}

		flipChunk(lastContigProcessed, lastChunkProcessed, currentChunk, currentContig[0], true, null);

		shutdownChunkWriteExecutor();

		saveInfoStores();

		splitInfoStoresByColumn();

		convertInfoStoresToByteIndexed();

		if (logger.isDebugEnabled()) {
			// Log out the first and last 50 variants
			int[] count = { 0 };
			for (String contig : store.getVariantMaskStorage().keySet()) {
				ArrayList<Integer> chunkIds = new ArrayList<>();
				FileBackedByteIndexedStorage<Integer, ConcurrentHashMap<String, VariableVariantMasks>> chromosomeStorage = store.getVariantMaskStorage()
						.get(contig);
				if (chromosomeStorage != null) {
					// print out the top and bottom 50 variants in the store (that have masks)
					chunkIds.addAll(chromosomeStorage.keys());
					for (Integer chunkId : chunkIds) {
						for (String variantSpec : chromosomeStorage.get(chunkId).keySet()) {
							count[0]++;
							VariableVariantMasks variantMasks = chromosomeStorage.get(chunkId).get(variantSpec);
							if (variantMasks != null) {
								VariantMask heterozygousMask = variantMasks.heterozygousMask;
								String heteroIdList = sampleIdsForMask(allSampleIds, heterozygousMask);
								VariantMask homozygousMask = variantMasks.homozygousMask;
								String homoIdList = sampleIdsForMask(allSampleIds, homozygousMask);

								if (!heteroIdList.isEmpty() && heteroIdList.length() < 1000)
									logger.debug(variantSpec + " : heterozygous : " + heteroIdList);
								if (!homoIdList.isEmpty() && homoIdList.length() < 1000)
									logger.debug(variantSpec + " : homozygous : " + homoIdList);
							}
						}
						if (count[0] > 50)
							break;
					}

					count[0] = 0;
					for (int x = chunkIds.size() - 1; x > 0; x--) {
						int chunkId = chunkIds.get(x);
						chromosomeStorage.get(chunkId).keySet().forEach((variantSpec) -> {
							count[0]++;
							VariableVariantMasks variantMasks = chromosomeStorage.get(chunkId).get(variantSpec);
							if (variantMasks != null) {
								VariantMask heterozygousMask = variantMasks.heterozygousMask;
								String heteroIdList = sampleIdsForMask(allSampleIds, heterozygousMask);
								VariantMask homozygousMask = variantMasks.homozygousMask;
								String homoIdList = sampleIdsForMask(allSampleIds, homozygousMask);

								if (!heteroIdList.isEmpty() && heteroIdList.length() < 1000)
									logger.debug(variantSpec + " : heterozygous : " + heteroIdList);
								if (!homoIdList.isEmpty() && homoIdList.length() < 1000)
									logger.debug(variantSpec + " : homozygous : " + homoIdList);
							}
						});
						if (count[0] > 50)
							break;
					}
				}
			}
		}

		store.setVariantSpecIndex(variantIndexBuilder.getVariantSpecIndex().toArray(new String[0]));
		saveVariantStore(store, variantMaskStorage);
	}

	private void createWalkers() {
		List<VCFIndexLine> vcfIndexLines = parseVCFIndex(indexFile);
		for (VCFIndexLine line : vcfIndexLines) {
			walkers.add(new VCFWalker(line));
		}
	}

	protected String sampleIdsForMask(String[] sampleIds, VariantMask variantMask) {
		StringBuilder idList = new StringBuilder();
		if (variantMask != null) {
			if (variantMask instanceof VariantMaskBitmaskImpl) {
				BigInteger mask = ((VariantMaskBitmaskImpl) variantMask).getBitmask();
				for (int x = 0; x < mask.bitLength() - 4; x++) {
					if (variantMask.testBit(x)) {
						idList.append(sampleIds[x]).append(",");
					}
				}
			} else if (variantMask instanceof VariantMaskSparseImpl) {
				for (Integer patientIndex : ((VariantMaskSparseImpl) variantMask).getPatientIndexes()) {
					idList.append(sampleIds[patientIndex]).append(",");
				}
			}
		}
		return idList.toString();
	}

	protected void flipChunk(String lastContigProcessed, int lastChunkProcessed, int currentChunk,
			String currentContig, boolean isLastChunk, String currentLine) throws IOException, FileNotFoundException {
		if (!currentContig.contentEquals(lastContigProcessed) || isLastChunk) {
			if (infoStoreFlipped.get(lastContigProcessed) == null || !infoStoreFlipped.get(lastContigProcessed)) {
				infoStoreFlipped.put(lastContigProcessed, true);
				File infoFile = new File(storageDir, lastContigProcessed + "_infoStores.javabin");
				logger.debug(Thread.currentThread().getName() + " : " + "Flipping info : "
						+ infoFile.getAbsolutePath() + " " + lastContigProcessed + " ");
				try (FileOutputStream fos = new FileOutputStream(infoFile);
						GZIPOutputStream gzos = new GZIPOutputStream(fos);
						ObjectOutputStream oos = new ObjectOutputStream(gzos);) {
					oos.writeObject(infoStoreMap);
				}
				ConcurrentHashMap<String, InfoStore> newInfoStores = new ConcurrentHashMap<String, InfoStore>();
				for (String key : infoStoreMap.keySet()) {
					newInfoStores.put(key, new InfoStore(infoStoreMap.get(key).description, ";", key));
				}
				infoStoreMap = newInfoStores;
			}
			if (currentLine != null) {
				String[] split = currentLine.split("\t");
				if (split[8].length() > 2) {
					contigIsHemizygous = false;
				} else {
					contigIsHemizygous = true;
				}
			}
		}
		if (!currentContig.contentEquals(lastContigProcessed) || currentChunk > lastChunkProcessed || isLastChunk) {
			// flip chunk
			TreeMap<String, FileBackedJsonIndexStorage<Integer, ConcurrentHashMap<String, VariableVariantMasks>>> variantMaskStorage_f = variantMaskStorage;
			HashMap<String, char[][]> zygosityMaskStrings_f = zygosityMaskStrings;
			String lastContigProcessed_f = lastContigProcessed;
			int lastChunkProcessed_f = lastChunkProcessed;
			chunkWriteEx.execute(() -> {
				try {
					if (variantMaskStorage_f.get(lastContigProcessed_f) == null) {
						String fileName = lastContigProcessed_f + "masks.bin";
						if (!fileName.startsWith("chr")) {
							fileName = "chr" + fileName;
						}

						variantMaskStorage_f.put(lastContigProcessed_f,
								new FileBackedStorageVariantMasksImpl(new File(storageDir, fileName)));
					}
					variantMaskStorage_f.get(lastContigProcessed_f).put(lastChunkProcessed_f,
							convertLoadingMapToMaskMap(zygosityMaskStrings_f));
				} catch (IOException e) {
					throw new UncheckedIOException(e);
				}
			});
			if (lastChunkProcessed % 100 == 0) {
				logger.info(System.currentTimeMillis() + " Done loading chunk : " + lastChunkProcessed
						+ " for chromosome " + lastContigProcessed);
			}
			zygosityMaskStrings = new HashMap<String/* variantSpec */, char[][]/* string bitmasks */>();
		}
	}

	protected void saveVariantStore(VariantStore store,
			TreeMap<String, FileBackedJsonIndexStorage<Integer, ConcurrentHashMap<String, VariableVariantMasks>>> variantMaskStorage)
			throws IOException, FileNotFoundException {
		store.setVariantMaskStorage(variantMaskStorage);
		for (FileBackedByteIndexedStorage<Integer, ConcurrentHashMap<String, VariableVariantMasks>> storage : variantMaskStorage
				.values()) {
			if (storage != null)
				storage.complete();
		}
		store.writeInstance(storageDirStr);
		logger.debug("Done saving variant masks.");
	}

	protected void saveInfoStores() throws IOException, FileNotFoundException {
		logger.debug("Saving info" + (System.currentTimeMillis() - startTime) + " seconds");
		try (FileOutputStream fos = new FileOutputStream(new File(storageDir, "infoStores.javabin"));
				GZIPOutputStream gzos = new GZIPOutputStream(fos);
				ObjectOutputStream oos = new ObjectOutputStream(gzos);) {
			oos.writeObject(infoStoreMap);
		}
		logger.debug("Done saving info.");
		logger.info("completed load in " + (System.currentTimeMillis() - startTime) + " seconds");
	}

	public void splitInfoStoresByColumn() throws FileNotFoundException, IOException {
		logger.debug("Splitting" + (System.currentTimeMillis() - startTime) + " seconds");
		try {
			VCFPerPatientInfoStoreSplitter.splitAll(storageDir,  new File(mergedDirStr));
		} catch (ClassNotFoundException | ExecutionException | InterruptedException e) {
			throw new RuntimeException(e);
		}
		logger.debug("Split" + (System.currentTimeMillis() - startTime) + " seconds");
	}

	public void convertInfoStoresToByteIndexed() throws FileNotFoundException, IOException {
		logger.debug("Converting" + (System.currentTimeMillis() - startTime) + " seconds");
		try {
			VCFPerPatientInfoStoreToFBBIISConverter.convertAll(mergedDirStr, storageDirStr);
		} catch (ClassNotFoundException | ExecutionException | InterruptedException e) {
			throw new RuntimeException(e);
		}
		logger.debug("Converted " + ((System.currentTimeMillis() - startTime) / 1000) + " seconds");
	}

	protected void shutdownChunkWriteExecutor() {
		chunkWriteEx.shutdown();
		while (!chunkWriteEx.isTerminated()) {
			try {
				logger.debug(((ThreadPoolExecutor) chunkWriteEx).getQueue().size() + " writes left to be written.");
				chunkWriteEx.awaitTermination(20, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				logger.error("Error shutting down chunk write executor", e);
			}
		}
	}

	private static ConcurrentHashMap<String, VariableVariantMasks> convertLoadingMapToMaskMap(
			HashMap<String, char[][]> zygosityMaskStrings_f) {
		ConcurrentHashMap<String, VariableVariantMasks> maskMap = new ConcurrentHashMap<>(zygosityMaskStrings_f.size());
		zygosityMaskStrings_f.entrySet().parallelStream().forEach((entry) -> {
			maskMap.put(entry.getKey(), new VariableVariantMasks(entry.getValue()));
		});
		return maskMap;
	}

	static TreeMap<String, Boolean> infoStoreFlipped = new TreeMap<String, Boolean>();

	protected class VCFWalker implements Comparable<VCFWalker> {

		protected List<Integer> indices;
		protected Integer[] vcfOffsets;
		protected Integer[] bitmaskOffsets;
		protected HashMap<Integer, Integer> vcfIndexLookup;
		protected String currentLine;
		protected String[] currentLineSplit;
		protected BufferedReader vcfReader;
		protected VCFIndexLine vcfIndexLine;
		boolean hasNext = true;
		String currentContig;
		Integer currentPosition;
		String currentRef, currentAlt;

		public VCFWalker(VCFIndexLine vcfIndexLine) {
			this.vcfIndexLine = vcfIndexLine;
			this.vcfOffsets = new Integer[vcfIndexLine.patientIds.length];
			this.bitmaskOffsets = new Integer[vcfIndexLine.patientIds.length];
			vcfIndexLookup = new HashMap<Integer, Integer>(vcfOffsets.length);
			indices = new ArrayList<>();
			for (int x = 0; x < vcfOffsets.length; x++) {
				indices.add(x);
			}
			try {
				InputStream in = this.vcfIndexLine.isGzipped
						? new BlockCompressedInputStream(new FileInputStream(this.vcfIndexLine.vcfPath))
						: new FileInputStream(this.vcfIndexLine.vcfPath);
				InputStreamReader reader = new InputStreamReader(in);
				vcfReader = new BufferedReader(reader, 1024 * 1024 * 32);
			} catch (IOException e) {
				throw new RuntimeException("File not found, please fix the VCF index file : " + vcfIndexLine, e);
			}
		}

		public void updateRecords(char[][] zygosityMaskStrings, ConcurrentHashMap<String, InfoStore> infoStores) {
			int[] startOffsetForLine = { 0 };
			int columnNumber = 0;
			int formatStartIndex = 0;
			for (startOffsetForLine[0] = 0; columnNumber <= 8; startOffsetForLine[0]++) {
				if (currentLine.charAt(startOffsetForLine[0]) == '\t') {
					columnNumber++;
					if (columnNumber == 8) {
						formatStartIndex = startOffsetForLine[0];
					}
				}
			}

			// Because formatStartIndex is the index of the preceding \t and startOffsetForLine is
			// after the next \t, this has to be 4 even though "\tGT" is only 3 characters.
			boolean formatIsGTOnly = (startOffsetForLine[0] - formatStartIndex) == 4;

			if (!formatIsGTOnly) {
				// index is sample index in vcf
				int index = 0;
				try {

					int currentLineOffset = startOffsetForLine[0];
					// added index boundary condition from master
					for (; index < indices.size() && currentLineOffset < currentLine.length(); currentLineOffset++) {
						if (currentLine.charAt(currentLineOffset - 1) == '\t') {
							// and index lookup from variant explorer branch
							if (vcfIndexLookup.containsKey(index)) {
								setMasksForSample(zygosityMaskStrings, vcfIndexLookup.get(index), currentLineOffset);
							}
							index++;
						}
					}

				} catch (IndexOutOfBoundsException e) {
					throw new RuntimeException("INDEX out of bounds " + currentLine.substring(0, Math.min(50, currentLine.length())),
							e);
				}
			} else {
				indices.parallelStream().forEach((index) -> {
					setMasksForSample(zygosityMaskStrings, index, startOffsetForLine[0] + vcfOffsets[index]);
				});
			}
			String[] infoColumns = currentLineSplit[7].split("[;&]");
			String currentSpecNotation = currentSpecNotation();
			int variantIndex = variantIndexBuilder.getIndex(currentSpecNotation);
			infoStores.values().parallelStream().forEach(infoStore -> {
				infoStore.processRecord(variantIndex, infoColumns);
			});
		}

		private void setMasksForSample(char[][] zygosityMaskStrings, int index, int startOffsetForSample) {
			int patientZygosityIndex = (currentLine.charAt(startOffsetForSample + 0)
					+ currentLine.charAt(startOffsetForSample + 1) + currentLine.charAt(startOffsetForSample + 2)) % 7;
			zygosityMaskStrings[patientZygosityIndex][bitmaskOffsets[index]] = '1';
		}

		protected String currentSpecNotation() {
			String[] variantInfo = currentLineSplit[7].split("[=;]");
			String gene = "NULL";
			String consequence = "NULL";
			for (int i = 0; i < variantInfo.length; i = i + 2) {
				if ("Gene_with_variant".equals(variantInfo[i])) {
					gene = variantInfo[i + 1];
				}
				if ("Variant_consequence_calculated".equals(variantInfo[i])) {
					consequence = variantInfo[i + 1];
				}
			}
			return currentLineSplit[0] + "," + currentLineSplit[1] + "," + currentLineSplit[3] + ","
					+ currentLineSplit[4] + "," + gene + "," + consequence;
		}

		public void readHeaders(ConcurrentHashMap<String, InfoStore> infoStores) throws IOException {
			nextLine();
			// Parse VCF header extracting only INFO columns until we hit the header row for the TSV portion
			while (currentLine != null && currentLine.startsWith("##")) {
				if (currentLine.startsWith("##INFO") && vcfIndexLine.isAnnotated) {
					String[] info = currentLine.replaceAll("##INFO=<", "").replaceAll(">", "").split(",");
					info[3] = String.join(",", Arrays.copyOfRange(info, 3, info.length));
					String columnKey = info[0].split("=")[1];
					if (!infoStores.containsKey(columnKey)) {
						logger.info("Creating info store " + currentLine);
						InfoStore infoStore = new InfoStore(info[3], ";", columnKey);
						infoStores.put(columnKey, infoStore);
					}
				}
				nextLine();
			}

			// Make sure all expected columns are included and in the right order, stop the presses otherwise
			if (!currentLine.startsWith("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\t")) {
				logger.error("Invalid VCF format [" + this.vcfIndexLine.vcfPath
						+ "], please ensure all expected columns are included per VCF 4.1 specification : #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\t");
				System.exit(-1);
			}

			String[] vcfHeaderSamples = Arrays.copyOfRange(currentLineSplit, 9, currentLineSplit.length - 1);

			// Set vcf offsets by sampleIds
			for (int x = 0; x < vcfIndexLine.sampleIds.length; x++) {
				// y is index into vcf samples
				// x is index into patient IDs
				int y;
				for (y = 0; y < vcfHeaderSamples.length
						&& !vcfHeaderSamples[y].contentEquals(vcfIndexLine.sampleIds[x]); y++)
					;
				vcfOffsets[x] = y * 4;
				vcfIndexLookup.put(y, x);
			}

			// add not GT only logic

			nextLine();
		}

		public void setBitmaskOffsets(Integer[] allPatientIdsSorted) {
			for (int x = 0; x < vcfIndexLine.patientIds.length; x++) {
				bitmaskOffsets[x] = Arrays.binarySearch(allPatientIdsSorted, vcfIndexLine.patientIds[x]);
			}
			
		}

		public void nextLine() throws IOException {
			currentLine = vcfReader.readLine();
			if (currentLine == null) {
				hasNext = false;
			} else if (currentLine.startsWith("#CHROM")) {
				currentLineSplit = currentLine.split("\t");
			} else if (!currentLine.startsWith("#")) {
				currentLineSplit = new String[8];
				int start = 0;
				int end = 0;
				for (int x = 0; x < currentLineSplit.length; x++) {
					while (currentLine.charAt(end) != '\t') {
						end++;
					}
					currentLineSplit[x] = currentLine.substring(start, end);
					end++;
					start = end;
				}
				currentContig = currentLineSplit[0];
				currentPosition = Integer.parseInt(currentLineSplit[1]);
				currentRef = currentLineSplit[3];
				currentAlt = currentLineSplit[4];
			}
		}

		private static LinkedList<String> contigOrder = new LinkedList<String>();

		@Override
		public int compareTo(VCFWalker o) {
			int chromosomeCompared;
			if (currentContig.contentEquals(o.currentContig)) {
				chromosomeCompared = 0;
				if (!contigOrder.contains(currentContig)) {
					contigOrder.add(currentContig);
				}
			} else {
				Integer currentContigIndex = -1;
				Integer oCurrentContigIndex = -1;
				for (int x = 0; x < contigOrder.size(); x++) {
					if (currentContig.contentEquals(contigOrder.get(x))) {
						currentContigIndex = x;
					}
					if (o.currentContig.contentEquals(contigOrder.get(x))) {
						oCurrentContigIndex = x;
					}
				}
				if (currentContigIndex == -1) {
					contigOrder.add(currentContig);
					currentContigIndex = contigOrder.size() - 1;
				}
				if (oCurrentContigIndex == -1) {
					contigOrder.add(o.currentContig);
					oCurrentContigIndex = contigOrder.size() - 1;
				}
				chromosomeCompared = currentContigIndex.compareTo(oCurrentContigIndex);
			}
			if (chromosomeCompared == 0) {
				int positionCompared = currentPosition.compareTo(o.currentPosition);
				if (positionCompared == 0) {
					int refCompared = currentRef.compareTo(o.currentRef);
					if (refCompared == 0) {
						return currentAlt.compareTo(o.currentAlt);
					}
					return refCompared;
				}
				return positionCompared;
			}
			return chromosomeCompared;
		}
	}

	private static final int FILE_COLUMN = 0;
	private static final int CHROMOSOME_COLUMN = 1;
	private static final int ANNOTATED_FLAG_COLUMN = 2;
	private static final int GZIP_FLAG_COLUMN = 3;
	private static final int SAMPLE_IDS_COLUMN = 4;
	private static final int PATIENT_IDS_COLUMN = 5;
	// These columns are to support a future feature, ignore them for now.
	private static final int SAMPLE_RELATIONSHIPS_COLUMN = 6;
	private static final int RELATED_SAMPLE_IDS_COLUMN = 7;

	protected static class VCFIndexLine implements Comparable<VCFIndexLine> {
		String vcfPath;
		String contig;
		boolean isAnnotated;
		boolean isGzipped;
		String[] sampleIds;
		Integer[] patientIds;

		@Override
		public int compareTo(VCFIndexLine o) {
			int chomosomeComparison = o.contig == null ? 1 : contig == null ? 0 : contig.compareTo(o.contig);
			if (chomosomeComparison == 0) {
				return vcfPath.compareTo(o.vcfPath);
			}
			return chomosomeComparison;
		}
	}

	private static List<VCFIndexLine> parseVCFIndex(File vcfIndexFile) {
		TreeSet<VCFIndexLine> vcfSet = new TreeSet<>();
		CSVParser parser;
		try {
			parser = CSVParser.parse(vcfIndexFile, Charset.forName("UTF-8"),
					CSVFormat.DEFAULT.withDelimiter('\t').withSkipHeaderRecord(true));

			final boolean[] horribleHeaderSkipFlag = { false };
			parser.forEach((CSVRecord r) -> {
				if (horribleHeaderSkipFlag[0]) {
					VCFIndexLine line = new VCFIndexLine();
					line.vcfPath = r.get(FILE_COLUMN).trim();
					line.contig = r.get(CHROMOSOME_COLUMN).trim().contentEquals("ALL") ? null
							: r.get(CHROMOSOME_COLUMN).trim();
					line.isAnnotated = Integer.parseInt(r.get(ANNOTATED_FLAG_COLUMN).trim()) == 1;
					line.isGzipped = Integer.parseInt(r.get(GZIP_FLAG_COLUMN).trim()) == 1;
					line.sampleIds = Arrays.stream(r.get(SAMPLE_IDS_COLUMN).split(",")).map(String::trim)
							.collect(Collectors.toList()).toArray(new String[0]);
					line.patientIds = Arrays.stream(r.get(PATIENT_IDS_COLUMN).split(",")).map(String::trim)
							.map(Integer::parseInt).collect(Collectors.toList()).toArray(new Integer[0]);
					vcfSet.add(line);
				} else {
					horribleHeaderSkipFlag[0] = true;
				}
			});
		} catch (IOException e) {
			throw new UncheckedIOException("IOException caught parsing vcfIndexFile", e);
		}
		return new ArrayList<>(vcfSet);
	}

}

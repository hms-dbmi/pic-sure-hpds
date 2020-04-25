package edu.harvard.hms.dbmi.avillach.hpds.etl.genotype;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.SequenceInputStream;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.harvard.hms.dbmi.avillach.hpds.data.genotype.InfoStore;
import edu.harvard.hms.dbmi.avillach.hpds.data.genotype.VariantMasks;
import edu.harvard.hms.dbmi.avillach.hpds.data.genotype.VariantSpec;
import edu.harvard.hms.dbmi.avillach.hpds.data.genotype.VariantStore;
import edu.harvard.hms.dbmi.avillach.hpds.storage.FileBackedByteIndexedStorage;
import htsjdk.samtools.util.BlockCompressedInputStream;

public class NewVCFLoader {

	static Logger logger = Logger.getLogger(NewVCFLoader.class);
	static File storageDir = null;
	// DO NOT CHANGE THIS unless you want to reload all the data everywhere.
	private static int CHUNK_SIZE = 1000;

	public static void main(String[] args) throws FileNotFoundException, IOException {
		File indexFile = new File("/opt/local/hpds/vcfIndex.tsv");
		storageDir = new File("/opt/local/hpds/all");
		loadVCFs(indexFile);

	}

	private static ExecutorService chunkWriteEx = Executors.newFixedThreadPool(1);

	private static ConcurrentHashMap<String, InfoStore> infoStoreMap = new ConcurrentHashMap<String, InfoStore>();

	private static HashMap<String, char[][]> zygosityMaskStrings;

	private static FileBackedByteIndexedStorage<Integer, ConcurrentHashMap<String, VariantMasks>>[] variantMaskStorage = 
			new FileBackedByteIndexedStorage[24];

	private static long startTime;

	private static List<VCFWalker> walkers = new ArrayList<>();

	private static void loadVCFs(File indexFile) throws IOException {
		startTime = System.currentTimeMillis();
		List<VCFIndexLine> vcfIndexLines = parseVCFIndex(indexFile);
		for(VCFIndexLine line : vcfIndexLines) {
			walkers.add(new VCFWalker(line));
		}
		TreeSet<Integer> allPatientIds = new TreeSet<Integer>();

		// Pull the INFO columns out of the headers for each walker and add all patient ids
		walkers.parallelStream().forEach(walker -> {
			try {
				walker.readHeaders(infoStoreMap);
				allPatientIds.addAll(Arrays.asList(walker.vcfIndexLine.patientIds));
			} catch (IOException e) {
				logger.error(e);
				System.exit(-1);
			}});
		String[] allSampleIds = new String[allPatientIds.size()];
		walkers.parallelStream().forEach(walker -> {
			walker.setBitmaskOffsets(allPatientIds.toArray(new Integer[0]));
			Integer[] patientIds = allPatientIds.toArray(new Integer[0]);
			for(int x = 0; x < walker.vcfIndexLine.sampleIds.length; x++) {
				allSampleIds[Arrays.binarySearch(patientIds, walker.vcfIndexLine.patientIds[x])] = walker.vcfIndexLine.sampleIds[x];
			}
		});

		VariantStore store = new VariantStore();
		store.setPatientIds(allPatientIds.stream().map((id)->{return id.toString();}).collect(Collectors.toList()).toArray(new String[0]));

		int lastChromosomeProcessed = 0;
		int lastChunkProcessed = 0;
		int currentChunk = 0;
		int[] currentChromosome = {-1};
		int[] currentPosition = {-1};
		String[] currentRef = new String[1];
		String[] currentAlt = new String[1];

		zygosityMaskStrings = new HashMap<String/*variantSpec*/, char[][]/*string bitmasks*/>();

		List<Integer> positionsProcessedInChunk = new ArrayList<>();
		while(walkers.parallelStream().anyMatch(walker ->{return walker.hasNext;})){
			Collections.sort(walkers);
			VCFWalker lowestWalker = walkers.get(0);
			String currentSpecNotation = lowestWalker.currentSpecNotation();
			currentChromosome[0] = lowestWalker.currentChromosome;
			currentPosition[0] = lowestWalker.currentPosition;
			currentRef[0] = lowestWalker.currentRef;
			currentAlt[0] = lowestWalker.currentAlt;
			currentChunk = lowestWalker.currentPosition/CHUNK_SIZE;
			positionsProcessedInChunk.add(currentPosition[0]);

			flipChunk(lastChromosomeProcessed, lastChunkProcessed, currentChunk, currentChromosome, false);
			lastChromosomeProcessed = lowestWalker.currentChromosome;
			lastChunkProcessed = currentChunk;

			char[][][] maskStringsForVariantSpec = {zygosityMaskStrings.get(currentSpecNotation)};
			if(maskStringsForVariantSpec[0] == null) {
				maskStringsForVariantSpec[0] = new char[7][allPatientIds.size()];
				for(int x = 0;x<6;x++) {
					maskStringsForVariantSpec[0][x] = new char[allPatientIds.size()];
					for(int y = 0;y<allPatientIds.size();y++) {
						maskStringsForVariantSpec[0][x][y]='0';
					}
				}
			}
			walkers.stream().filter((walker)->{
				return 
						walker.currentPosition == currentPosition[0] && 
						walker.currentAlt == currentAlt[0] && 
						walker.currentRef == currentRef[0] && 
						walker.currentChromosome == currentChromosome[0];
			}).forEach((walker)->{
				walker.updateRecords(maskStringsForVariantSpec[0], infoStoreMap);
				try {
					walker.nextLine();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			});
			zygosityMaskStrings.put(currentSpecNotation, maskStringsForVariantSpec[0]);
		}
		flipChunk(lastChromosomeProcessed, lastChunkProcessed, currentChunk, currentChromosome, true);

		shutdownChunkWriteExecutor();

		saveVariantStore(store, variantMaskStorage);

		saveInfoStores();

		splitInfoStoresByColumn();

		convertInfoStoresToByteIndexed();

		if(Level.DEBUG.equals(logger.getEffectiveLevel())) {
			int[] count = {0};
			Integer[] allPatientIdsArray = allPatientIds.toArray(new Integer[0]);
			Integer[] patientIds = Arrays.stream(store.getPatientIds()).map((id)->{return Integer.parseInt(id);}).collect(Collectors.toList()).toArray(new Integer[0]);
			for(int chromosome = 0;chromosome<store.variantMaskStorage.length;chromosome++) {
				ArrayList<Integer> chunkIds = new ArrayList<>();
				FileBackedByteIndexedStorage<Integer, ConcurrentHashMap<String, VariantMasks>> chromosomeStorage = store.variantMaskStorage[chromosome];
				if(chromosomeStorage!=null) {
					chunkIds.addAll(chromosomeStorage.keys());
					for(Integer chunkId : chunkIds){
						for(String variantSpec : chromosomeStorage.get(chunkId).keySet()){
							try {
								count[0]++;
								VariantMasks variantMasks = chromosomeStorage.get(chunkId).get(variantSpec);
								if(variantMasks!=null) {
									BigInteger heterozygousMask = variantMasks.heterozygousMask;
									String heteroIdList = sampleIdsForMask(allSampleIds, heterozygousMask);
									BigInteger homozygousMask = variantMasks.homozygousMask;
									String homoIdList = sampleIdsForMask(allSampleIds, homozygousMask);

									if(!heteroIdList.isEmpty() && heteroIdList.length()<1000)logger.debug(variantSpec + " : heterozygous : " + heteroIdList);
									if(!homoIdList.isEmpty() && homoIdList.length()<1000)logger.debug(variantSpec + " : homozygous : " + homoIdList);
								}
							} catch (IOException e) {
								logger.error(e);
							}
						}
						if(count[0]>50)break;
					};
					count[0] = 0;
					for(int x = chunkIds.size()-1;x>0;x--) {
						int chunkId = chunkIds.get(x);
						chromosomeStorage.get(chunkId).keySet().forEach((variantSpec)->{
							try {
								count[0]++;
								VariantMasks variantMasks = chromosomeStorage.get(chunkId).get(variantSpec);
								if(variantMasks!=null) {
									BigInteger heterozygousMask = variantMasks.heterozygousMask;
									String heteroIdList = sampleIdsForMask(allSampleIds, heterozygousMask);
									BigInteger homozygousMask = variantMasks.homozygousMask;
									String homoIdList = sampleIdsForMask(allSampleIds, homozygousMask);

									if(!heteroIdList.isEmpty() && heteroIdList.length()<1000)logger.debug(variantSpec + " : heterozygous : " + heteroIdList);
									if(!homoIdList.isEmpty() && homoIdList.length()<1000)logger.debug(variantSpec + " : homozygous : " + homoIdList);
								}
							} catch (IOException e) {
								logger.error(e);
							}
						});
						if(count[0]>50)break;
					}
				}
			}
		}
	}

	private static String sampleIdsForMask(String[] sampleIds, BigInteger heterozygousMask) {
		String idList = "";
		if(heterozygousMask!=null) {
			for(int x = 2;x<heterozygousMask.bitLength()-2;x++) {
				if(heterozygousMask.testBit(heterozygousMask.bitLength() - 1 - x)) {
					idList+=sampleIds[x-2]+",";
				}
			}
		}
		return idList;
	}

	private static String patientIdsForMask(Integer[] patientIds, BigInteger heterozygousMask) {
		String idList = "";
		if(heterozygousMask!=null) {
			for(int x = 2;x<heterozygousMask.bitLength()-2;x++) {
				if(heterozygousMask.testBit(heterozygousMask.bitLength() - 1 - x)) {
					idList+=patientIds[x-2]+",";
				}
			}
		}
		return idList;
	}

	private static void flipChunk(int lastChromosomeProcessed, int lastChunkProcessed, int currentChunk,
			int[] currentChromosome, boolean isLastChunk) throws IOException, FileNotFoundException {
		if(currentChromosome[0] > lastChromosomeProcessed || isLastChunk) {
			if(!infoStoreFlipped[lastChromosomeProcessed]) {
				infoStoreFlipped[lastChromosomeProcessed] = true;
				File infoFile = new File(storageDir, lastChromosomeProcessed + "_infoStores.javabin");
				System.out.println(Thread.currentThread().getName() + " : " + "Flipping info : " + infoFile.getAbsolutePath() + " " + lastChromosomeProcessed + " ");
				try (
						FileOutputStream fos = new FileOutputStream(infoFile);
						GZIPOutputStream gzos = new GZIPOutputStream(fos);
						ObjectOutputStream oos = new ObjectOutputStream(gzos);			
						){
					oos.writeObject(infoStoreMap);
				}
				ConcurrentHashMap<String, InfoStore> newInfoStores = new ConcurrentHashMap<String, InfoStore>();
				for(String key : infoStoreMap.keySet()) {
					newInfoStores.put(key, new InfoStore(infoStoreMap.get(key).description, ";", key));
				}
				infoStoreMap = newInfoStores;
			}
			walkers = walkers.parallelStream().filter((walker)->{
				return walker.hasNext;
			}).collect(Collectors.toList());
		}
		if(currentChromosome[0] > lastChromosomeProcessed || currentChunk > lastChunkProcessed || isLastChunk) {
			// flip chunk
			FileBackedByteIndexedStorage<Integer, ConcurrentHashMap<String, VariantMasks>>[] variantMaskStorage_f = variantMaskStorage;
			HashMap<String, char[][]> zygosityMaskStrings_f = zygosityMaskStrings;
			int lastChromosomeProcessed_f = lastChromosomeProcessed;
			int lastChunkProcessed_f = lastChunkProcessed;
			chunkWriteEx.execute(()->{
				try {
					if(variantMaskStorage_f[lastChromosomeProcessed_f]==null) {
						variantMaskStorage_f[lastChromosomeProcessed_f] = 
								new FileBackedByteIndexedStorage(Integer.class, ConcurrentHashMap.class, 
										new File(storageDir, "chr" + lastChromosomeProcessed_f + "masks.bin"));							
					}
					variantMaskStorage_f[lastChromosomeProcessed_f].put(lastChunkProcessed_f, convertLoadingMapToMaskMap(zygosityMaskStrings_f));
				} catch (IOException e) {
					logger.error(e);
				}
			});
			if(lastChunkProcessed % 100 == 0) {
				logger.info(System.currentTimeMillis() + " Done loading chunk : " + lastChunkProcessed + " for chromosome " + lastChromosomeProcessed);
			}
			zygosityMaskStrings = new HashMap<String/*variantSpec*/, char[][]/*string bitmasks*/>();
		}
	}

	private static void saveVariantStore(VariantStore store,
			FileBackedByteIndexedStorage<Integer, ConcurrentHashMap<String, VariantMasks>>[] variantMaskStorage)
					throws IOException, FileNotFoundException {
		store.variantMaskStorage = variantMaskStorage;
		for(FileBackedByteIndexedStorage<Integer, ConcurrentHashMap<String, VariantMasks>> storage : variantMaskStorage) {
			if(storage!=null)
				storage.complete();
		}
		try (
				FileOutputStream fos = new FileOutputStream(new File(storageDir, "variantStore.javabin"));
				GZIPOutputStream gzos = new GZIPOutputStream(fos);
				ObjectOutputStream oos = new ObjectOutputStream(gzos);				
				){
			oos.writeObject(store);			
		}
		store = null;
		variantMaskStorage = null;
		logger.debug("Done saving variant masks.");
	}

	private static void saveInfoStores()
			throws IOException, FileNotFoundException {
		logger.debug("Saving info" + (System.currentTimeMillis() - startTime) + " seconds");
		try (
				FileOutputStream fos = new FileOutputStream(new File(storageDir, "infoStores.javabin"));
				GZIPOutputStream gzos = new GZIPOutputStream(fos);
				ObjectOutputStream oos = new ObjectOutputStream(gzos);			
				){
			oos.writeObject(infoStoreMap);
		}
		logger.debug("Done saving info.");
		logger.info("completed load in " + (System.currentTimeMillis() - startTime) + " seconds");
	}

	private static void splitInfoStoresByColumn() throws FileNotFoundException, IOException {
		logger.debug("Splitting" + (System.currentTimeMillis() - startTime) + " seconds");
		try {
			VCFPerPatientInfoStoreSplitter.splitAll();
		} catch (ClassNotFoundException | InterruptedException | ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		logger.debug("Split" + (System.currentTimeMillis() - startTime) + " seconds");
	}

	private static void convertInfoStoresToByteIndexed() throws FileNotFoundException, IOException {
		logger.debug("Converting" + (System.currentTimeMillis() - startTime) + " seconds");
		try {
			VCFPerPatientInfoStoreToFBBIISConverter.convertAll("/opt/local/hpds/merged", "/opt/local/hpds/all");
		} catch (ClassNotFoundException | InterruptedException | ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		logger.debug("Converted " + ((System.currentTimeMillis() - startTime)/1000) + " seconds");
	}

	private static void shutdownChunkWriteExecutor() {
		chunkWriteEx.shutdown();
		while(!chunkWriteEx.isTerminated()) {
			try {
				logger.debug(((ThreadPoolExecutor)chunkWriteEx).getQueue().size() + " writes left to be written.");
				chunkWriteEx.awaitTermination(20, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			
		}
	}

	private static ConcurrentHashMap<String, VariantMasks> convertLoadingMapToMaskMap(
			HashMap<String, char[][]> zygosityMaskStrings_f) {
		ConcurrentHashMap<String, VariantMasks> maskMap = new ConcurrentHashMap<>();
		zygosityMaskStrings_f.entrySet().parallelStream().forEach((entry)->{
			maskMap.put(entry.getKey(), new VariantMasks(entry.getValue()));
		});
		return maskMap;
	}

	static boolean[] infoStoreFlipped = new boolean[24];

	private static class VCFWalker implements Comparable<VCFWalker>{

		private List<Integer> indices;
		private Integer[] vcfOffsets;
		private Integer[] bitmaskOffsets;
		private String currentLine;
		private String[] currentLineSplit;
		private BufferedReader vcfReader;
		private VCFIndexLine vcfIndexLine;
		boolean hasNext = true;
		Integer currentChromosome, currentPosition;
		String currentRef, currentAlt;

		public VCFWalker(VCFIndexLine vcfIndexLine) {
			this.vcfIndexLine = vcfIndexLine;
			this.vcfOffsets = new Integer[vcfIndexLine.patientIds.length];
			this.bitmaskOffsets = new Integer[vcfIndexLine.patientIds.length];
			indices = new ArrayList<>();
			for(int x = 0;x<vcfOffsets.length;x++) {
				indices.add(x);
			}
			try{
				InputStream in = this.vcfIndexLine.isGzipped ? 
						new BlockCompressedInputStream(new FileInputStream(this.vcfIndexLine.vcfPath)) : 
							new FileInputStream(this.vcfIndexLine.vcfPath);
						InputStreamReader reader = new InputStreamReader(in);
						vcfReader = new BufferedReader(reader, 1024 * 1024 * 32);
			} catch (FileNotFoundException e) {
				throw new RuntimeException("File not found, please fix the VCF index file : " + vcfIndexLine, e);
			}
		}

		public void updateRecords(char[][] zygosityMaskStrings, ConcurrentHashMap<String, InfoStore> infoStores) {
			int[] startOffsetForLine = {0};
			int columnNumber = 0;
			for(startOffsetForLine[0] = 0;columnNumber<=8;startOffsetForLine[0]++) {
				if(currentLine.charAt(startOffsetForLine[0])=='\t') {
					columnNumber++;
				}
			}
			indices.parallelStream().forEach((index)->{
				int startOffsetForSample = startOffsetForLine[0] + vcfOffsets[index];
				int patientZygosityIndex = 
						(
								currentLine.charAt(startOffsetForSample  + 0) + 
								currentLine.charAt(startOffsetForSample  + 1) + 
								currentLine.charAt(startOffsetForSample  + 2)
								) % 7;
				zygosityMaskStrings[patientZygosityIndex][bitmaskOffsets[index]] = '1';
			});
			infoStores.values().parallelStream().forEach(infoStore->{
				infoStore.processRecord(currentSpecNotation(), currentLineSplit[7].split("[;&]"));
			});
		}

		private String currentSpecNotation() {
			return currentLineSplit[0] + "," 
					+ currentLineSplit[1] + "," + 
					currentLineSplit[3]+ "," + currentLineSplit[4];
		}

		public void readHeaders(ConcurrentHashMap<String, InfoStore> infoStores) throws IOException {
			nextLine();
			// Parse VCF header extracting only INFO columns until we hit the header row for the TSV portion
			while(currentLine!=null && currentLine.startsWith("##")) {
				if(currentLine.startsWith("##INFO") && vcfIndexLine.isAnnotated){
					String[] info = currentLine.replaceAll("##INFO=<", "").replaceAll(">", "").split(",");
					info[3] = String.join(",", Arrays.copyOfRange(info, 3, info.length));
					String columnKey = info[0].split("=")[1];
					if(! infoStores.containsKey(columnKey)) {
						logger.info("Creating info store " + currentLine);
						InfoStore infoStore = new InfoStore(info[3], ";", columnKey);
						infoStores.put(columnKey, infoStore);										
					}
				}
				nextLine();
			}

			// Make sure all expected columns are included and in the right order, stop the presses otherwise
			if(!currentLine.startsWith("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\t")) {
				logger.error("Invalid VCF format, please ensure all expected columns are included per VCF 4.1 specification : #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\t");
				System.exit(-1);
			}

			String[] vcfHeaderSamples = Arrays.copyOfRange(currentLineSplit,9,currentLineSplit.length-1);

			// Set vcf offsets by sampleIds
			for(int x = 0;x < vcfIndexLine.sampleIds.length;x++) {
				int y;
				for(y = 0;y < vcfHeaderSamples.length && ! vcfHeaderSamples[y].contentEquals(vcfIndexLine.sampleIds[x]);y++);
				vcfOffsets[x] = y * 4;
			}
			nextLine();
		}

		public void setBitmaskOffsets(Integer[] allPatientIdsSorted) {
			for(int x = 0; x<vcfIndexLine.patientIds.length; x++) {
				bitmaskOffsets[x] = Arrays.binarySearch(allPatientIdsSorted, vcfIndexLine.patientIds[x]);
			}
		}

		public void nextLine() throws IOException {
			currentLine = vcfReader.readLine();
			if(currentLine == null) {
				hasNext = false;
			} else if(currentLine.startsWith("#CHROM")){
				currentLineSplit = currentLine.split("\t");
			} else if(!currentLine.startsWith("#")){
				currentLineSplit = new String[8];
				int start = 0;
				int end = 0;
				for(int x = 0; x<currentLineSplit.length; x++) {
					while(currentLine.charAt(end)!='\t') {
						end++;
					}
					currentLineSplit[x] = currentLine.substring(start,end);
					end++;
					start = end;
				}
				currentChromosome = Integer.parseInt(currentLineSplit[0]);
				currentPosition = Integer.parseInt(currentLineSplit[1]);
				currentRef = currentLineSplit[3];
				currentAlt = currentLineSplit[4];
			}
		}

		@Override
		public int compareTo(VCFWalker o) {
			int chromosomeCompared = currentChromosome.compareTo(o.currentChromosome);
			if(chromosomeCompared == 0) {
				int positionCompared = currentPosition.compareTo(o.currentPosition);
				if(positionCompared == 0) {
					int refCompared = currentRef.compareTo(o.currentRef);
					if(refCompared == 0) {
						return currentAlt.compareTo(o.currentAlt);
					}
					return refCompared;
				}
				return positionCompared;
			}
			return chromosomeCompared;
		}
	}


	private static final int 
	FILE_COLUMN = 0,
	CHROMOSOME_COLUMN = 1,
	ANNOTATED_FLAG_COLUMN = 2,
	GZIP_FLAG_COLUMN=3,
	SAMPLE_IDS_COLUMN=4,
	PATIENT_IDS_COLUMN=5,
	//These columns are to support a future feature, ignore them for now.
	SAMPLE_RELATIONSHIPS_COLUMN=6,
	RELATED_SAMPLE_IDS_COLUMN=7
	;

	private static class VCFIndexLine implements Comparable<VCFIndexLine>{
		String vcfPath;
		Integer chromosome;
		boolean isAnnotated;
		boolean isGzipped;
		String[] sampleIds;
		Integer[] patientIds;

		@Override
		public int compareTo(VCFIndexLine o) {
			int chomosomeComparison = chromosome.compareTo(o.chromosome);
			if(chomosomeComparison==0) {
				return vcfPath.compareTo(o.vcfPath);
			}
			return chomosomeComparison;
		}
	}

	private static List<VCFIndexLine> parseVCFIndex(File vcfIndexFile){
		TreeSet<VCFIndexLine> vcfSet = new TreeSet<>();
		CSVParser parser;
		try {
			parser = CSVParser.parse(vcfIndexFile, Charset.forName("UTF-8"), CSVFormat.DEFAULT.withDelimiter('\t').withSkipHeaderRecord(true));

			final boolean[] horribleHeaderSkipFlag = {false};
			parser.forEach((CSVRecord r)->{
				if(horribleHeaderSkipFlag[0]) {
					VCFIndexLine line = new VCFIndexLine();
					line.vcfPath = r.get(FILE_COLUMN).trim();
					line.chromosome = r.get(CHROMOSOME_COLUMN).trim().contentEquals("ALL") ? 
							0 : Integer.parseInt(r.get(CHROMOSOME_COLUMN).trim());
					line.isAnnotated = Integer.parseInt(r.get(ANNOTATED_FLAG_COLUMN).trim())==1;
					line.isGzipped = Integer.parseInt(r.get(GZIP_FLAG_COLUMN).trim())==1;
					line.sampleIds = Arrays.stream(r.get(SAMPLE_IDS_COLUMN).split(",")).map(String::trim).collect(Collectors.toList()).toArray(new String[0]);
					line.patientIds = Arrays.stream(r.get(PATIENT_IDS_COLUMN).split(",")).map(String::trim).map(Integer::parseInt).collect(Collectors.toList()).toArray(new Integer[0]);
					vcfSet.add(line);
				} else {
					horribleHeaderSkipFlag[0] = true;
				}
			});
		} catch (IOException e) {
			throw new RuntimeException("IOException caught parsing vcfIndexFile", e);
		}
		return new ArrayList<>(vcfSet);
	}

}
package edu.harvard.hms.dbmi.avillach.hpds.etl;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.*;

import edu.harvard.hms.dbmi.avillach.hpds.crypto.Crypto;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.ColumnMeta;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.KeyAndValue;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.PhenoCube;
 
public class LoadingStore {
 
	public RandomAccessFile allObservationsStore;

	TreeMap<String, ColumnMeta> metadataMap = new TreeMap<>();
	
	private static Logger log = LoggerFactory.getLogger(LoadingStore.class);
	
	public LoadingCache<String, PhenoCube> store = CacheBuilder.newBuilder()
			.maximumSize(16)
			.removalListener(new RemovalListener<String, PhenoCube>() {

				@Override
				public void onRemoval(RemovalNotification<String, PhenoCube> arg0) {
					log.debug("removing " + arg0.getKey());
					if(arg0.getValue().getLoadingMap()!=null) {
						complete(arg0.getValue());
					}
					try {
						PhenoCube<?> cube = arg0.getValue();
						ColumnMeta columnMeta = new ColumnMeta()
								.setName(arg0.getKey())
								.setWidthInBytes(cube.getColumnWidth())
								.setCategorical(cube.isStringType());

						long startOffset = allObservationsStore.getFilePointer();
						columnMeta.setAllObservationsOffset(startOffset);

						KeyAndValue<?>[] sortedByKey = cube.sortedByKey();
						columnMeta.setObservationCount(sortedByKey.length);

						// Optimized patient count: single pass over sorted-by-key array
						// Relies on sortedByKey being sorted by patient ID
						int patientCount = 0;
						if (sortedByKey.length > 0) {
							patientCount = 1;
							int prevKey = sortedByKey[0].getKey();
							for (int i = 1; i < sortedByKey.length; i++) {
								int currentKey = sortedByKey[i].getKey();
								if (currentKey != prevKey) {
									patientCount++;
									prevKey = currentKey;
								}
							}
						}
						columnMeta.setPatientCount(patientCount);

						if(columnMeta.isCategorical()) {
							// Optimized: reduced allocations for category values
							List<?> keyBasedArray = cube.keyBasedArray();
							TreeSet<String> uniqueCategories = new TreeSet<>();
							for (Object val : keyBasedArray) {
								uniqueCategories.add((String) val);
							}
							columnMeta.setCategoryValues(new ArrayList<>(uniqueCategories));
						} else {
							// Optimized: single-pass min/max without intermediate list
							Object[] values = cube.keyBasedArray().toArray();
							double min = Double.MAX_VALUE;
							double max = Double.MIN_VALUE;
							for (Object val : values) {
								double d = (Double) val;
								if (d < min) min = d;
								if (d > max) max = d;
							}
							columnMeta.setMin(min);
							columnMeta.setMax(max);
						}

						// Optimized: pre-sized ByteArrayOutputStream + try-with-resources
						// Estimate: typical cube serialization is 1KB-10KB, cap at 64KB initial size
						int estimatedSize = Math.min(sortedByKey.length * 20, 65536);
						byte[] encryptedData;
						try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream(estimatedSize);
						     ObjectOutputStream out = new ObjectOutputStream(byteStream)) {
							out.writeObject(cube);
							out.flush();
							encryptedData = Crypto.encryptData(byteStream.toByteArray());
						}

						allObservationsStore.write(encryptedData);
						long endOffset = allObservationsStore.getFilePointer();

						// NOTE: Despite the name, setAllObservationsLength stores the END OFFSET, not length.
						// All readers compute length as: getAllObservationsLength() - getAllObservationsOffset()
						// This is kept for backward compatibility with existing data files and readers.
						columnMeta.setAllObservationsLength(endOffset);
						metadataMap.put(columnMeta.getName(), columnMeta);
					} catch (IOException e) {
						throw new UncheckedIOException(e);
					}
				}

				private <V extends Comparable<V>> void complete(PhenoCube<V> cube) {
					ArrayList<KeyAndValue<V>> entryList = new ArrayList<KeyAndValue<V>>(
							cube.getLoadingMap().stream().map((entry)->{
								return new KeyAndValue<V>(entry.getKey(), entry.getValue(), entry.getTimestamp());
							}).collect(Collectors.toList()));

					List<KeyAndValue<V>> sortedByKey = entryList.stream()
							.sorted(Comparator.comparing(KeyAndValue<V>::getKey))
							.collect(Collectors.toList());
					cube.setSortedByKey(sortedByKey.toArray(new KeyAndValue[0]));

					if(cube.isStringType()) {
						TreeMap<V, List<Integer>> categoryMap = new TreeMap<>();
						for(KeyAndValue<V> entry : cube.sortedByValue()) {
							if(!categoryMap.containsKey(entry.getValue())) {
								categoryMap.put(entry.getValue(), new LinkedList<Integer>());
							}
							categoryMap.get(entry.getValue()).add(entry.getKey());
						}
						TreeMap<V, TreeSet<Integer>> categorySetMap = new TreeMap<>();
						categoryMap.entrySet().stream().forEach((entry)->{
							categorySetMap.put(entry.getKey(), new TreeSet<Integer>(entry.getValue()));
						});
						cube.setCategoryMap(categorySetMap);
					}

				}
			})
			.build(
					new CacheLoader<String, PhenoCube>() {
						public PhenoCube load(String key) throws Exception {
							log.debug("Loading cube: " + key);
							return null;
						}
					});

	public TreeSet<Integer> allIds = new TreeSet<Integer>();
	
	public void saveStore(String hpdsDirectory) throws IOException {
		log.info("Invalidating store");
		store.invalidateAll();
		store.cleanUp();

		try {
			log.info("Writing metadata");
			try (FileOutputStream fos = new FileOutputStream(hpdsDirectory + "columnMeta.javabin");
			     GZIPOutputStream gzos = new GZIPOutputStream(fos);
			     ObjectOutputStream metaOut = new ObjectOutputStream(gzos)) {
				metaOut.writeObject(metadataMap);
				metaOut.writeObject(allIds);
				metaOut.flush();
			}
			log.info("Metadata written successfully");
		} finally {
			// Ensure allObservationsStore is always closed even if metadata write fails
			log.info("Closing allObservationsStore");
			if (allObservationsStore != null) {
				allObservationsStore.close();
			}
		}

		dumpStatsAndColumnMeta(hpdsDirectory);
	}

	/**
	 * Dumps statistics using the default HPDS directory path.
	 * Calls dumpStats(String) with "/opt/local/hpds/".
	 */
	public void dumpStats() {
		dumpStats("/opt/local/hpds/");
	}

	/**
	 * Dumps statistics from the specified HPDS directory.
	 * @param hpdsDirectory Path to HPDS directory (must end with /)
	 */
	public void dumpStats(String hpdsDirectory) {
		log.info("Dumping Stats from: {}", hpdsDirectory);
		try (ObjectInputStream objectInputStream = new ObjectInputStream(
				new GZIPInputStream(new FileInputStream(hpdsDirectory + "columnMeta.javabin")))) {
			TreeMap<String, ColumnMeta> metastore = (TreeMap<String, ColumnMeta>) objectInputStream.readObject();
			Set<Integer> allIds = (TreeSet<Integer>) objectInputStream.readObject();

			long totalNumberOfObservations = 0;

			System.out.println("\n\nConceptPath\tObservationCount\tMinNumValue\tMaxNumValue\tCategoryValues");
			for(String key : metastore.keySet()) {
				ColumnMeta columnMeta = metastore.get(key);
				System.out.println(String.join("\t", key.toString(), columnMeta.getObservationCount()+"",
						columnMeta.getMin()==null ? "NaN" : columnMeta.getMin().toString(),
								columnMeta.getMax()==null ? "NaN" : columnMeta.getMax().toString(),
										columnMeta.getCategoryValues() == null ? "NUMERIC CONCEPT" : String.join(",",
												columnMeta.getCategoryValues()
												.stream().map((value)->{return value==null ? "NULL_VALUE" : "\""+value+"\"";}).collect(Collectors.toList()))));
				totalNumberOfObservations += columnMeta.getObservationCount();
			}

			System.out.println("Total Number of Concepts : " + metastore.size());
			System.out.println("Total Number of Patients : " + allIds.size());
			System.out.println("Total Number of Observations : " + totalNumberOfObservations);

		} catch (IOException | ClassNotFoundException e) {
			throw new RuntimeException("Could not load metastore from " + hpdsDirectory, e);
		}
	}

	/**
	 * This method will display counts for the objects stored in the metadata.
	 * This will also write out a csv file used by the data dictionary importer.
	 */
    public void dumpStatsAndColumnMeta(String hpdsDirectory) {
        try (ObjectInputStream objectInputStream =
                     new ObjectInputStream(new GZIPInputStream(new FileInputStream(hpdsDirectory + "columnMeta.javabin")))) {

            @SuppressWarnings("unchecked")
            TreeMap<String, ColumnMeta> metastore = (TreeMap<String, ColumnMeta>) objectInputStream.readObject();

            try (BufferedWriter writer = Files.newBufferedWriter(
                    Paths.get(hpdsDirectory + "columnMeta.csv"),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING);
                 CSVPrinter printer = new CSVPrinter(writer, CSVFormat.RFC4180)) {

                // header
                printer.printRecord(
                        "name",
                        "widthInBytes",
                        "columnOffset",
                        "categorical",
                        "categoryValues",
                        "min",
                        "max",
                        "allObservationsOffset",
                        "allObservationsLength",
                        "observationCount",
                        "patientCount"
                );

                for (ColumnMeta columnMeta : metastore.values()) {
                    String categoryValues = null;
                    if (columnMeta.getCategoryValues() != null && !columnMeta.getCategoryValues().isEmpty()) {
                        categoryValues = String.join("µ", columnMeta.getCategoryValues());
                    }

                    printer.printRecord(
                            columnMeta.getName(),
                            columnMeta.getWidthInBytes(),
                            columnMeta.getColumnOffset(),
                            columnMeta.isCategorical(),
                            categoryValues,
                            columnMeta.getMin(),                 // null -> empty field (not "null")
                            columnMeta.getMax(),                 // null -> empty field (not "null")
                            columnMeta.getAllObservationsOffset(),
                            columnMeta.getAllObservationsLength(),
                            columnMeta.getObservationCount(),
                            columnMeta.getPatientCount()
                    );
                }
            }

        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Could not load metastore", e);
        }
    }

}

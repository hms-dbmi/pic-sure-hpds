package edu.harvard.hms.dbmi.avillach.hpds.writer;

import com.google.common.cache.*;
import edu.harvard.hms.dbmi.avillach.hpds.crypto.Crypto;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.ColumnMeta;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.KeyAndValue;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.PhenoCube;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static edu.harvard.hms.dbmi.avillach.hpds.crypto.Crypto.DEFAULT_KEY_NAME;

/**
 * Enhanced LoadingStore with spool-and-finalize semantics.
 *
 * Key improvement over standard LoadingStore:
 * - Allows multiple partial batches for the same conceptPath during ingestion
 * - Spools partial data to disk instead of writing directly to allObservationsStore
 * - At finalize time, merges all partials per concept and writes exactly once
 *
 * This removes the need for global concept ordering and enables safe interleaving of sources.
 *
 * Invariant: Each conceptPath is written exactly ONCE to allObservationsStore, but observations
 * for that concept may arrive in multiple batches from different sources.
 */
public class SpoolingLoadingStore {
    private static final Logger log = LoggerFactory.getLogger(SpoolingLoadingStore.class);

    private final Path spoolDirectory;
    private final String outputDirectory;
    private final String encryptionKeyName;
    private final int cacheSize;

    // Null sentinel detector for identifying string representations of missing data
    private final NullSentinelDetector nullSentinelDetector;

    // Track metadata for each concept (may be updated across batches)
    private final ConcurrentHashMap<String, ConceptMetadata> conceptMetadata = new ConcurrentHashMap<>();

    // Track all patient IDs encountered
    private final Set<Integer> allIds = Collections.synchronizedSet(new TreeSet<>());

    // In-memory cache for active concepts
    private final LoadingCache<String, PhenoCube> cache;

    // Track spool files for cleanup
    private final ConcurrentHashMap<String, List<Path>> spoolFiles = new ConcurrentHashMap<>();

    // Concept-level locks for parallel observation writes (replaces global synchronized)
    private final ConcurrentHashMap<String, ReentrantLock> conceptLocks = new ConcurrentHashMap<>();

    /**
     * Metadata tracked per concept during ingestion.
     */
    private static class ConceptMetadata {
        boolean isCategorical;
        int columnWidth;
        int totalPartialCount = 0; // Track number of spooled partials

        ConceptMetadata(boolean isCategorical, int columnWidth) {
            this.isCategorical = isCategorical;
            this.columnWidth = columnWidth;
        }
    }

    public SpoolingLoadingStore(Path spoolDirectory, String outputDirectory, String encryptionKeyName, int cacheSize) {
        this.spoolDirectory = spoolDirectory;
        // Ensure outputDirectory ends with /
        this.outputDirectory = outputDirectory.endsWith("/") ? outputDirectory : outputDirectory + "/";
        this.encryptionKeyName = encryptionKeyName;
        this.cacheSize = cacheSize;

        // Initialize null sentinel detector with default sentinels
        this.nullSentinelDetector = new NullSentinelDetector();

        try {
            Files.createDirectories(spoolDirectory);
            Files.createDirectories(Path.of(this.outputDirectory));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create spool/output directories", e);
        }

        this.cache = CacheBuilder.newBuilder()
            .maximumSize(cacheSize)
            .removalListener(new RemovalListener<String, PhenoCube>() {
                @Override
                public void onRemoval(RemovalNotification<String, PhenoCube> notification) {
                    if (notification.getCause() == RemovalCause.SIZE || notification.getCause() == RemovalCause.EXPLICIT) {
                        String conceptPath = notification.getKey();
                        PhenoCube cube = notification.getValue();
                        if (cube != null && cube.getLoadingMap() != null && !cube.getLoadingMap().isEmpty()) {
                            log.debug("Spooling concept: {} ({} observations)", conceptPath, cube.getLoadingMap().size());
                            spoolPartial(conceptPath, cube);
                        }
                    }
                }
            })
            .build(new CacheLoader<String, PhenoCube>() {
                @Override
                public PhenoCube load(String key) {
                    ConceptMetadata meta = conceptMetadata.get(key);
                    if (meta == null) {
                        throw new IllegalStateException("No metadata for concept: " + key);
                    }
                    Class<?> valueType = meta.isCategorical ? String.class : Double.class;
                    PhenoCube cube = new PhenoCube(key, valueType);
                    cube.setColumnWidth(meta.columnWidth);
                    return cube;
                }
            });
    }

    /**
     * Default constructor with standard paths.
     */
    public SpoolingLoadingStore() {
        this(
            Path.of("/opt/local/hpds/spool"),
            "/opt/local/hpds/",
            DEFAULT_KEY_NAME,
            16
        );
    }

    /**
     * Accepts an observation and adds it to the appropriate concept's cube.
     * The cube may be spooled to disk if cache eviction occurs.
     *
     * Thread-safe: Uses concept-level locking to allow concurrent writes to different concepts.
     */
    public void addObservation(int patientNum, String conceptPath, Comparable<?> value, Date timestamp) {
        // Acquire lock for THIS concept only (allows concurrent writes to different concepts)
        ReentrantLock lock = conceptLocks.computeIfAbsent(conceptPath, k -> new ReentrantLock());
        lock.lock();
        try {
            allIds.add(patientNum);

        // Ensure metadata exists for this concept (determined by first observation's type)
        ConceptMetadata meta = conceptMetadata.computeIfAbsent(conceptPath, k -> {
            boolean isCategorical = value instanceof String;
            int width = isCategorical ? 80 : 8; // Default widths
            return new ConceptMetadata(isCategorical, width);
        });

        // Handle type mismatch: coerce value to match concept's established type
        // This handles cases where Parquet auto-detection produces mixed types for same column
        Comparable<?> coercedValue = value;

        if (meta.isCategorical && value instanceof Double) {
            // Concept is categorical but received numeric value - convert to string
            coercedValue = value.toString();
        } else if (!meta.isCategorical && value instanceof String) {
            // Concept is numeric but received string value

            // Check if this is a null sentinel (e.g., "None", "nan", "null", etc.)
            // Null sentinels should be skipped entirely, not treated as conversion errors
            if (nullSentinelDetector.isNullSentinel(value)) {
                // This is a recognized null sentinel - skip this observation entirely
                log.debug("Skipping null sentinel '{}' for numeric concept '{}'", value, conceptPath);
                return; // Don't add to cube, don't promote to categorical
            }

            // Not a null sentinel - try to parse as Double
            try {
                coercedValue = Double.parseDouble((String) value);
            } catch (NumberFormatException e) {
                // Cannot parse as numeric AND not a null sentinel - must promote concept to categorical
                log.warn("Concept '{}' was initially numeric but received non-numeric String '{}' - promoting to categorical",
                    conceptPath, value);

                // Promote to categorical and convert all future values
                ConceptMetadata newMeta = new ConceptMetadata(true, 80);
                conceptMetadata.put(conceptPath, newMeta);

                // Spool existing numeric cube before type change
                // This ensures we don't have mixed types in same cube
                PhenoCube existingCube = cache.getIfPresent(conceptPath);
                if (existingCube != null && existingCube.getLoadingMap() != null && !existingCube.getLoadingMap().isEmpty()) {
                    spoolPartial(conceptPath, existingCube);
                }
                cache.invalidate(conceptPath);

                // Use string value as-is
                coercedValue = value;
            }
        }

            try {
                PhenoCube cube = cache.get(conceptPath);
                cube.add(patientNum, coercedValue, timestamp);
            } catch (Exception e) {
                throw new RuntimeException("Failed to add observation for concept: " + conceptPath, e);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Spools a partial cube to disk (compressed).
     * Multiple partials for the same concept are allowed.
     */
    private void spoolPartial(String conceptPath, PhenoCube cube) {
        try {
            ConceptMetadata meta = conceptMetadata.get(conceptPath);
            if (meta == null) {
                throw new IllegalStateException("No metadata for concept: " + conceptPath);
            }

            // Create spool file (one per partial)
            int partialNum = meta.totalPartialCount++;
            Path spoolFile = spoolDirectory.resolve(sanitizeFilename(conceptPath) + ".partial." + partialNum + ".gz");

            spoolFiles.computeIfAbsent(conceptPath, k -> Collections.synchronizedList(new ArrayList<>())).add(spoolFile);

            // Write the loading map (raw KeyAndValue list) to spool
            try (OutputStream fos = Files.newOutputStream(spoolFile);
                 GZIPOutputStream gzos = new GZIPOutputStream(fos);
                 ObjectOutputStream oos = new ObjectOutputStream(gzos)) {
                oos.writeObject(cube.getLoadingMap());
            }

            log.debug("Spooled partial for concept {} to {} ({} observations)",
                conceptPath, spoolFile, cube.getLoadingMap().size());

        } catch (IOException e) {
            throw new UncheckedIOException("Failed to spool partial for concept: " + conceptPath, e);
        }
    }

    /**
     * Finalizes all concepts and writes to allObservationsStore.
     * This is where each concept is written exactly once.
     */
    public void saveStore() throws IOException {
        log.info("Finalizing store: flushing cache and merging spooled partials");

        // Flush cache (spools remaining concepts)
        cache.invalidateAll();
        cache.cleanUp();

        // Now merge and finalize each concept in deterministic order
        TreeMap<String, ColumnMeta> metadataMap = new TreeMap<>();

        try (RandomAccessFile allObservationsStore = new RandomAccessFile(outputDirectory + "allObservationsStore.javabin", "rw")) {
            log.info("=====Finalizing concepts=====");

            for (String conceptPath : new TreeSet<>(conceptMetadata.keySet())) {
                log.debug("Finalizing concept: {}", conceptPath);

                ConceptMetadata meta = conceptMetadata.get(conceptPath);
                PhenoCube finalCube = mergePartials(conceptPath, meta);

                // Complete the cube (sort, build category maps, compute stats)
                complete(finalCube);

                // Write to allObservationsStore (exactly once)
                ColumnMeta columnMeta = writeToStore(allObservationsStore, conceptPath, finalCube, meta);
                metadataMap.put(conceptPath, columnMeta);
            }
        }

        // Write columnMeta.javabin
        log.info("Writing columnMeta.javabin");
        try (ObjectOutputStream metaOut = new ObjectOutputStream(
            new GZIPOutputStream(new FileOutputStream(outputDirectory + "columnMeta.javabin")))) {
            metaOut.writeObject(metadataMap);
            metaOut.writeObject(allIds);
        }

        // Write columnMeta.csv for data dictionary
        dumpColumnMetaCSV();

        // Cleanup spool files
        cleanupSpool();

        log.info("Store finalized: {} concepts, {} patients", metadataMap.size(), allIds.size());
    }

    /**
     * Merges all spooled partials for a concept into a single PhenoCube.
     * Uses external merge if needed (currently in-memory for simplicity).
     */
    @SuppressWarnings("unchecked")
    private PhenoCube mergePartials(String conceptPath, ConceptMetadata meta) throws IOException {
        List<Path> partials = spoolFiles.getOrDefault(conceptPath, Collections.emptyList());

        Class<?> valueType = meta.isCategorical ? String.class : Double.class;
        PhenoCube merged = new PhenoCube(conceptPath, valueType);
        merged.setColumnWidth(meta.columnWidth);

        List<KeyAndValue> allEntries = new ArrayList<>();

        // Read all partials
        for (Path partial : partials) {
            try (InputStream fis = Files.newInputStream(partial);
                 GZIPInputStream gzis = new GZIPInputStream(fis);
                 ObjectInputStream ois = new ObjectInputStream(gzis)) {
                List<KeyAndValue> entries = (List<KeyAndValue>) ois.readObject();

                // Handle type coercion: if concept was promoted to categorical but partials contain Doubles,
                // convert them to Strings to ensure consistent typing
                if (meta.isCategorical) {
                    for (KeyAndValue entry : entries) {
                        if (entry.getValue() instanceof Double) {
                            // Convert Double to String for categorical concept
                            entry.setValue(entry.getValue().toString());
                        }
                    }
                }

                allEntries.addAll(entries);
            } catch (ClassNotFoundException e) {
                throw new IOException("Failed to deserialize partial: " + partial, e);
            }
        }

        merged.setLoadingMap(allEntries);
        log.debug("Merged {} partials for concept {} ({} total observations)",
            partials.size(), conceptPath, allEntries.size());

        return merged;
    }

    /**
     * Completes a cube: sorts by key, builds category maps, computes min/max.
     * This logic is extracted from the original LoadingStore.
     */
    @SuppressWarnings("unchecked")
    private <V extends Comparable<V>> void complete(PhenoCube<V> cube) {
        List<KeyAndValue<V>> loadingMap = cube.getLoadingMap();
        if (loadingMap == null || loadingMap.isEmpty()) {
            log.warn("Empty loading map for concept: {}", cube.name);
            cube.setSortedByKey(new KeyAndValue[0]);
            return;
        }

        // Sort by key
        List<KeyAndValue<V>> sortedByKey = loadingMap.stream()
            .sorted(Comparator.comparing(KeyAndValue<V>::getKey))
            .collect(Collectors.toList());
        cube.setSortedByKey(sortedByKey.toArray(new KeyAndValue[0]));

        // Build category map for categorical concepts
        if (cube.isStringType()) {
            TreeMap<V, TreeSet<Integer>> categoryMap = new TreeMap<>();
            for (KeyAndValue<V> entry : cube.sortedByValue()) {
                categoryMap.computeIfAbsent(entry.getValue(), k -> new TreeSet<>()).add(entry.getKey());
            }
            cube.setCategoryMap(categoryMap);
        }
    }

    /**
     * Writes a finalized cube to allObservationsStore and returns ColumnMeta.
     */
    private ColumnMeta writeToStore(RandomAccessFile store, String conceptPath, PhenoCube cube, ConceptMetadata meta) throws IOException {
        ColumnMeta columnMeta = new ColumnMeta()
            .setName(conceptPath)
            .setWidthInBytes(meta.columnWidth)
            .setCategorical(meta.isCategorical);

        columnMeta.setAllObservationsOffset(store.getFilePointer());
        columnMeta.setObservationCount(cube.sortedByKey().length);
        columnMeta.setPatientCount(
            Arrays.stream(cube.sortedByKey())
                .map(KeyAndValue::getKey)
                .collect(Collectors.toSet())
                .size()
        );

        // Set category values or min/max
        if (meta.isCategorical) {
            columnMeta.setCategoryValues(
                new ArrayList<>(new TreeSet<>((List<String>) cube.keyBasedArray()))
            );
        } else {
            List<Double> values = (List<Double>) cube.keyBasedArray().stream()
                .map(v -> (Double) v)
                .collect(Collectors.toList());
            double min = values.stream().mapToDouble(Double::doubleValue).min().orElse(Double.NaN);
            double max = values.stream().mapToDouble(Double::doubleValue).max().orElse(Double.NaN);
            columnMeta.setMin(min);
            columnMeta.setMax(max);
        }

        // Serialize and encrypt
        try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
             ObjectOutputStream out = new ObjectOutputStream(byteStream)) {
            out.writeObject(cube);
            out.flush();
            store.write(Crypto.encryptData(encryptionKeyName, byteStream.toByteArray()));
        }

        columnMeta.setAllObservationsLength(store.getFilePointer());
        return columnMeta;
    }

    /**
     * Cleans up all spool files on success.
     */
    private void cleanupSpool() throws IOException {
        log.info("Cleaning up spool directory: {}", spoolDirectory);
        for (List<Path> files : spoolFiles.values()) {
            for (Path file : files) {
                Files.deleteIfExists(file);
            }
        }
        spoolFiles.clear();
    }

    /**
     * Sanitizes concept path for use as filename with length limit.
     *
     * Uses a hybrid approach: human-readable prefix (80 chars) + stable hash (32 chars)
     * to ensure filenames stay under filesystem limits while maintaining uniqueness.
     *
     * @param conceptPath The full concept path (may be very long)
     * @return A safe filename ≤ 113 characters
     */
    private String sanitizeFilename(String conceptPath) {
        // Extract readable prefix (first 80 chars, sanitized)
        String prefix = conceptPath
            .substring(0, Math.min(conceptPath.length(), 80))
            .replaceAll("[^a-zA-Z0-9._-]", "_");

        // Generate stable hash for uniqueness
        String hash;
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = digest.digest(conceptPath.getBytes(StandardCharsets.UTF_8));
            // Use URL-safe Base64 (no /, + chars that cause filesystem issues)
            hash = Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(hashBytes)
                .substring(0, 32); // First 32 chars = 192 bits of hash (collision-resistant)
        } catch (NoSuchAlgorithmException e) {
            // Fallback: use Java's hashCode (less collision-resistant but sufficient)
            log.warn("SHA-256 not available, using fallback hash for: {}", conceptPath);
            hash = Integer.toHexString(conceptPath.hashCode());
        }

        // Combine: prefix_hash
        // Max length: 80 + 1 + 32 = 113 chars (+ ".partial.0.gz" = ~130 total)
        return prefix + "_" + hash;
    }

    public Set<Integer> getAllIds() {
        return Collections.unmodifiableSet(allIds);
    }

    /**
     * Exports columnMeta.javabin to CSV format for data dictionary importer.
     * This method mirrors the logic from LoadingStore.dumpStatsAndColumnMeta().
     *
     * CSV format (11 columns, no header):
     * 0. Concept path
     * 1. Width in bytes
     * 2. Column offset (deprecated, always 0)
     * 3. Is categorical
     * 4. Category values (µ-delimited for categorical, empty for numeric)
     * 5. Min value
     * 6. Max value
     * 7. All observations offset
     * 8. All observations length
     * 9. Observation count
     * 10. Patient count
     */
    @SuppressWarnings("unchecked")
    public void dumpColumnMetaCSV() throws IOException {
        String columnMetaFile = outputDirectory + "columnMeta.javabin";
        String csvFile = outputDirectory + "columnMeta.csv";

        try (ObjectInputStream objectInputStream =
                new ObjectInputStream(new GZIPInputStream(new FileInputStream(columnMetaFile)));
             BufferedWriter writer = Files.newBufferedWriter(
                Paths.get(csvFile),
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING)) {

            TreeMap<String, ColumnMeta> metastore = (TreeMap<String, ColumnMeta>) objectInputStream.readObject();
            CSVPrinter printer = new CSVPrinter(writer, CSVFormat.DEFAULT);

            for (String key : metastore.keySet()) {
                ColumnMeta columnMeta = metastore.get(key);
                Object[] columnMetaOut = new Object[11];

                // Build category values string (µ-delimited)
                StringBuilder listQuoted = new StringBuilder();
                AtomicInteger x = new AtomicInteger(1);

                if (columnMeta.getCategoryValues() != null) {
                    if (!columnMeta.getCategoryValues().isEmpty()) {
                        columnMeta.getCategoryValues().forEach(string -> {
                            listQuoted.append(string);
                            if (x.get() != columnMeta.getCategoryValues().size()) {
                                listQuoted.append("µ");
                            }
                            x.incrementAndGet();
                        });
                    }
                }

                columnMetaOut[0] = columnMeta.getName();
                columnMetaOut[1] = String.valueOf(columnMeta.getWidthInBytes());
                columnMetaOut[2] = String.valueOf(columnMeta.getColumnOffset());
                columnMetaOut[3] = String.valueOf(columnMeta.isCategorical());
                columnMetaOut[4] = listQuoted.toString();
                columnMetaOut[5] = String.valueOf(columnMeta.getMin());
                columnMetaOut[6] = String.valueOf(columnMeta.getMax());
                columnMetaOut[7] = String.valueOf(columnMeta.getAllObservationsOffset());
                columnMetaOut[8] = String.valueOf(columnMeta.getAllObservationsLength());
                columnMetaOut[9] = String.valueOf(columnMeta.getObservationCount());
                columnMetaOut[10] = String.valueOf(columnMeta.getPatientCount());

                printer.printRecord(columnMetaOut);
            }

            writer.flush();
            log.info("Exported columnMeta.csv: {} concepts", metastore.size());

        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize columnMeta.javabin", e);
        }
    }
}

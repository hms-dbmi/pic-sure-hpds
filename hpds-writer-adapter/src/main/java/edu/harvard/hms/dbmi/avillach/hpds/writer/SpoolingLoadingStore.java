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
    private final int maxObservationsPerConcept;

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
        AtomicInteger observationCount = new AtomicInteger(0); // Track observations in current cache entry

        ConceptMetadata(boolean isCategorical, int columnWidth) {
            this.isCategorical = isCategorical;
            this.columnWidth = columnWidth;
        }
    }

    // Constants for adaptive degradation
    private static final long TARGET_SIZE_BYTES = 1_500_000_000L;  // 1.5GB (safer margin for 2GB encryption limit)
    private static final int MIN_SAMPLE_SIZE = 10_000;              // Minimum sample for size estimation
    private static final int MAX_SAMPLE_SIZE = 10_000_000;             // Maximum sample for size estimation
    private static final double SAMPLE_RATIO = 0.50;                // 1% sample for large concepts
    private static final double INITIAL_SAFETY_FACTOR = 0.50;       // 85% of target (conservative initial estimate)
    private static final int MAX_DEGRADATION_RETRIES = 3;           // Maximum verification retries

    public SpoolingLoadingStore(Path spoolDirectory, String outputDirectory, String encryptionKeyName, int cacheSize, int maxObservationsPerConcept) {
        this.spoolDirectory = spoolDirectory;
        // Ensure outputDirectory ends with /
        this.outputDirectory = outputDirectory.endsWith("/") ? outputDirectory : outputDirectory + "/";
        this.encryptionKeyName = encryptionKeyName;
        this.cacheSize = cacheSize;
        this.maxObservationsPerConcept = maxObservationsPerConcept;

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
            16,
            5_000_000  // Default: 5M observations per concept (~190MB)
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

        // Check if concept has exceeded observation limit - force eviction if needed
        if (meta.observationCount.get() >= maxObservationsPerConcept) {
            // Force eviction by invalidating from cache
            PhenoCube cube = cache.getIfPresent(conceptPath);
            if (cube != null && cube.getLoadingMap() != null && !cube.getLoadingMap().isEmpty()) {
                log.info("Concept '{}' reached limit ({} observations), spooling...",
                        conceptPath, meta.observationCount.get());
                spoolPartial(conceptPath, cube);
                cache.invalidate(conceptPath);
                meta.observationCount.set(0);  // Reset counter after spool
            }
        }

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
                meta.observationCount.incrementAndGet();  // Track observation count
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
     * Includes adaptive degradation to prevent 2GB serialization failures.
     */
    @SuppressWarnings("unchecked")
    private ColumnMeta writeToStore(RandomAccessFile store, String conceptPath, PhenoCube cube, ConceptMetadata meta) throws IOException {
        // STEP 1: Attempt adaptive degradation if needed
        PhenoCube finalCube = cube;
        try {
            finalCube = adaptiveDegradation(conceptPath, cube, meta);
        } catch (OutOfMemoryError oom) {
            log.error("✗ OutOfMemoryError during degradation for {}: {}", conceptPath, oom.getMessage());
            log.error("  Attempting emergency fallback: keep 1 observation per patient only");

            // Emergency fallback: minimal patient representation
            try {
                finalCube = emergencyDegradation(conceptPath, cube);
            } catch (Exception e) {
                log.error("✗✗ Emergency degradation also failed for {}", conceptPath, e);
                throw new IOException("Concept too large to serialize even with minimal degradation: " + conceptPath, oom);
            }
        }

        // STEP 2: Build ColumnMeta
        ColumnMeta columnMeta = new ColumnMeta()
            .setName(conceptPath)
            .setWidthInBytes(meta.columnWidth)
            .setCategorical(meta.isCategorical);

        columnMeta.setAllObservationsOffset(store.getFilePointer());
        columnMeta.setObservationCount(finalCube.sortedByKey().length);
        columnMeta.setPatientCount(
            Arrays.stream(finalCube.sortedByKey())
                .map(KeyAndValue::getKey)
                .collect(Collectors.toSet())
                .size()
        );

        // Set category values or min/max
        if (meta.isCategorical) {
            columnMeta.setCategoryValues(
                new ArrayList<>(new TreeSet<>((List<String>) finalCube.keyBasedArray()))
            );
        } else {
            List<Double> values = (List<Double>) finalCube.keyBasedArray().stream()
                .map(v -> (Double) v)
                .collect(Collectors.toList());
            double min = values.stream().mapToDouble(Double::doubleValue).min().orElse(Double.NaN);
            double max = values.stream().mapToDouble(Double::doubleValue).max().orElse(Double.NaN);
            columnMeta.setMin(min);
            columnMeta.setMax(max);
        }

        // STEP 3: Serialize and encrypt with size validation
        try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
             ObjectOutputStream out = new ObjectOutputStream(byteStream)) {
            out.writeObject(finalCube);
            out.flush();

            byte[] serialized = byteStream.toByteArray();
            log.debug("Serialized {} to {} bytes", conceptPath, serialized.length);

            // Hard limit check: AES-GCM encryption fails at ~2.14GB (Integer.MAX_VALUE)
            // Allow 100MB buffer for encryption overhead
            if (serialized.length > Integer.MAX_VALUE - 100_000_000) {
                log.error("✗ Concept {} serialized to {} bytes - EXCEEDS ENCRYPTION LIMIT ({})",
                        conceptPath, serialized.length, Integer.MAX_VALUE);
                throw new IOException(String.format(
                        "Concept %s too large for encryption: %d bytes (limit: ~%d). " +
                        "Adaptive degradation failed to reduce size sufficiently.",
                        conceptPath, serialized.length, Integer.MAX_VALUE));
            }

            if (serialized.length > TARGET_SIZE_BYTES) {
                log.warn("⚠ Concept {} serialized to {} bytes (exceeds {} target) - should not happen after verification",
                        conceptPath, serialized.length, TARGET_SIZE_BYTES);
            }

            store.write(Crypto.encryptData(encryptionKeyName, serialized));
        } catch (OutOfMemoryError oom) {
            log.error("✗ OutOfMemoryError during serialization for {} even after degradation", conceptPath);
            log.error("  Final cube size: {} observations, {} patients",
                    finalCube.sortedByKey().length,
                    columnMeta.getPatientCount());
            throw new IOException("Concept exceeded 2GB limit during serialization: " + conceptPath, oom);
        }

        columnMeta.setAllObservationsLength(store.getFilePointer());

        // Log success
        if (finalCube != cube) {
            log.warn("Concept {} written with degradation ({} obs → {} obs)",
                    conceptPath, cube.sortedByKey().length, finalCube.sortedByKey().length);
        } else {
            log.debug("Concept {} written successfully ({} obs)", conceptPath, finalCube.sortedByKey().length);
        }

        return columnMeta;
    }

    /**
     * Emergency degradation: keep only 1 observation per patient (most recent).
     * Used as last resort when adaptive degradation fails.
     */
    @SuppressWarnings("unchecked")
    private <V extends Comparable<V>> PhenoCube<V> emergencyDegradation(String conceptPath, PhenoCube<V> cube) {
        log.warn("→ Emergency degradation for {}: keeping 1 observation per patient", conceptPath);

        Map<Integer, List<KeyAndValue<V>>> byPatient = groupByPatient(cube);
        List<KeyAndValue<V>> minimal = new ArrayList<>(byPatient.size());

        for (Map.Entry<Integer, List<KeyAndValue<V>>> entry : byPatient.entrySet()) {
            List<KeyAndValue<V>> patientObs = entry.getValue();

            // Sort by timestamp descending, take most recent
            patientObs.sort((a, b) -> {
                Long tsA = a.getTimestamp();
                Long tsB = b.getTimestamp();
                if (tsA == null || tsB == null) return 0;
                return tsB.compareTo(tsA);
            });

            minimal.add(patientObs.get(0));
        }

        log.warn("✓ Emergency degradation complete: {} obs → {} obs (1 per patient)",
                cube.sortedByKey().length, minimal.size());

        return buildCubeFromSample(cube, minimal);
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

    // ========================================================================
    // ADAPTIVE DEGRADATION METHODS
    // ========================================================================

    /**
     * Adaptively degrades a concept to fit within 2GB serialization limit.
     * Uses adaptive sampling for better size estimation, then iteratively verifies
     * actual serialized size to guarantee encryption success.
     */
    @SuppressWarnings("unchecked")
    private <V extends Comparable<V>> PhenoCube<V> adaptiveDegradation(String conceptPath,
                                                                        PhenoCube<V> cube,
                                                                        ConceptMetadata meta) throws IOException {
        int totalObservations = cube.sortedByKey().length;

        // STEP 1: Adaptive sample size for better estimation
        // Use larger samples for larger concepts (up to 1% or 100K observations)
        int sampleSize = Math.min(MAX_SAMPLE_SIZE,
                                  Math.max(MIN_SAMPLE_SIZE,
                                          (int)(totalObservations * SAMPLE_RATIO)));

        long estimatedSize = estimateSerializedSize(cube, sampleSize);

        if (estimatedSize <= TARGET_SIZE_BYTES) {
            log.debug("Concept {} ({} obs) estimated at {} bytes with {} sample - no degradation needed",
                    conceptPath, totalObservations, estimatedSize, sampleSize);
            return cube;
        }

        // STEP 2: Calculate initial target observation count
        // Use conservative safety factor for initial estimate
        long targetObservations = (long) Math.floor(
                (double) TARGET_SIZE_BYTES / estimatedSize
                        * totalObservations
                        * INITIAL_SAFETY_FACTOR
        );

        // Ensure minimum: at least 1 observation per patient
        int patientCount = countUniquePatients(cube);
        targetObservations = Math.max(targetObservations, patientCount);

        log.warn("Concept {} requires degradation: {} obs → {} obs (estimated {} GB → {} GB target)",
                conceptPath,
                totalObservations,
                targetObservations,
                estimatedSize / 1_000_000_000.0,
                TARGET_SIZE_BYTES / 1_000_000_000.0);

        // STEP 3: Iterative degradation with actual size verification
        PhenoCube<V> degraded = stratifiedTemporalSample(cube, targetObservations, meta);

        for (int retry = 0; retry < MAX_DEGRADATION_RETRIES; retry++) {
            try {
                // Verify actual serialized size
                ByteArrayOutputStream testStream = new ByteArrayOutputStream();
                try (ObjectOutputStream out = new ObjectOutputStream(testStream)) {
                    out.writeObject(degraded);
                    out.flush();
                }

                long actualSize = testStream.size();

                if (actualSize <= TARGET_SIZE_BYTES) {
                    log.info("✓ Degradation successful after {} attempt(s): {} obs retained, {} bytes (target: {} bytes)",
                            retry + 1,
                            degraded.sortedByKey().length,
                            actualSize,
                            TARGET_SIZE_BYTES);
                    return degraded;
                }

                // Too large - reduce by 15% more and retry
                long newTarget = (long)(degraded.sortedByKey().length * 0.85);
                newTarget = Math.max(newTarget, patientCount);  // Maintain minimum

                log.warn("Retry {}/{}: {} bytes still exceeds target ({}), reducing to {} obs",
                        retry + 1,
                        MAX_DEGRADATION_RETRIES,
                        actualSize,
                        TARGET_SIZE_BYTES,
                        newTarget);

                degraded = stratifiedTemporalSample(degraded, newTarget, meta);

            } catch (OutOfMemoryError oom) {
                // Aggressive reduction on OOM during verification
                long emergencyTarget = degraded.sortedByKey().length / 2;
                emergencyTarget = Math.max(emergencyTarget, patientCount);

                log.error("OutOfMemoryError during verification (retry {}/{}), halving to {} obs",
                        retry + 1,
                        MAX_DEGRADATION_RETRIES,
                        emergencyTarget);

                degraded = stratifiedTemporalSample(degraded, emergencyTarget, meta);
            }
        }

        // Failed after all retries
        throw new IOException(String.format(
                "Failed to degrade concept %s below %d bytes after %d attempts. " +
                "Final size still too large for 2GB encryption limit.",
                conceptPath, TARGET_SIZE_BYTES, MAX_DEGRADATION_RETRIES));
    }

    /**
     * Estimates serialized size of a PhenoCube by sampling observations.
     */
    @SuppressWarnings("unchecked")
    private <V extends Comparable<V>> long estimateSerializedSize(PhenoCube<V> cube, int sampleSize) throws IOException {
        int totalObservations = cube.sortedByKey().length;

        if (totalObservations <= sampleSize) {
            // Small enough to serialize fully
            return measureSerializedSize(cube);
        }

        // Create sample cube
        PhenoCube<V> sample = createSampleCube(cube, sampleSize);

        // Measure sample size
        long sampleBytes = measureSerializedSize(sample);

        // Extrapolate to full size with overhead correction
        double bytesPerObservation = (double) sampleBytes / sampleSize;
        long linearEstimate = (long) (bytesPerObservation * totalObservations);
        long fixedOverhead = estimateFixedOverhead(cube);
        long correctedEstimate = linearEstimate + fixedOverhead;

        log.debug("Size estimation: {} sample bytes, {} bytes/obs, {} total estimated",
                sampleBytes, bytesPerObservation, correctedEstimate);

        return correctedEstimate;
    }

    /**
     * Creates a uniform sample of observations from a cube.
     */
    @SuppressWarnings("unchecked")
    private <V extends Comparable<V>> PhenoCube<V> createSampleCube(PhenoCube<V> cube, int sampleSize) {
        KeyAndValue<V>[] all = cube.sortedByKey();
        List<KeyAndValue<V>> sample = new ArrayList<>(sampleSize);

        // Uniform stride sampling
        double stride = (double) all.length / sampleSize;
        for (double i = 0; i < all.length && sample.size() < sampleSize; i += stride) {
            sample.add(all[(int) i]);
        }

        return buildCubeFromSample(cube, sample);
    }

    /**
     * Measures actual serialized size by serializing to ByteArrayOutputStream.
     */
    private <V extends Comparable<V>> long measureSerializedSize(PhenoCube<V> cube) {
        try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
             ObjectOutputStream out = new ObjectOutputStream(byteStream)) {
            out.writeObject(cube);
            out.flush();
            return byteStream.size();
        } catch (IOException e) {
            throw new RuntimeException("Failed to measure serialized size", e);
        }
    }

    /**
     * Estimates fixed overhead (Java serialization header, arrays, metadata).
     */
    private <V extends Comparable<V>> long estimateFixedOverhead(PhenoCube<V> cube) {
        long overhead = 200;  // Base overhead

        if (cube.isStringType() && cube.getCategoryMap() != null) {
            int categoryCount = cube.getCategoryMap().size();
            overhead += 100 + (categoryCount * 20);  // TreeMap overhead
        }

        return overhead;
    }

    /**
     * Stratified temporal sampling: guarantee patient coverage + temporal spread.
     */
    @SuppressWarnings("unchecked")
    private <V extends Comparable<V>> PhenoCube<V> stratifiedTemporalSample(PhenoCube<V> cube,
                                                                             long targetObs,
                                                                             ConceptMetadata meta) {
        // PHASE 1: Guarantee patient coverage
        Map<Integer, List<KeyAndValue<V>>> byPatient = groupByPatient(cube);
        int patientCount = byPatient.size();

        // Select one representative observation per patient (prefer most recent)
        List<KeyAndValue<V>> guaranteedSample = new ArrayList<>(patientCount);
        List<KeyAndValue<V>> remainingPool = new ArrayList<>();

        for (Map.Entry<Integer, List<KeyAndValue<V>>> entry : byPatient.entrySet()) {
            List<KeyAndValue<V>> patientObs = entry.getValue();

            // Sort by timestamp descending (most recent first)
            patientObs.sort((a, b) -> {
                Long tsA = a.getTimestamp();
                Long tsB = b.getTimestamp();
                if (tsA == null || tsB == null) return 0;
                return tsB.compareTo(tsA);  // Descending
            });

            // Take most recent observation as guaranteed
            guaranteedSample.add(patientObs.get(0));

            // Add rest to pool
            if (patientObs.size() > 1) {
                remainingPool.addAll(patientObs.subList(1, patientObs.size()));
            }
        }

        // PHASE 2: Fill remaining quota with temporal sampling
        long remainingQuota = targetObs - guaranteedSample.size();

        if (remainingQuota > 0 && !remainingPool.isEmpty()) {
            // Sort pool by timestamp
            remainingPool.sort((a, b) -> {
                Long tsA = a.getTimestamp();
                Long tsB = b.getTimestamp();
                if (tsA == null) return -1;
                if (tsB == null) return 1;
                return tsA.compareTo(tsB);
            });

            // Sample uniformly across time
            double stride = (double) remainingPool.size() / remainingQuota;
            for (double i = 0; i < remainingPool.size() && guaranteedSample.size() < targetObs; i += stride) {
                guaranteedSample.add(remainingPool.get((int) i));
            }
        }

        log.info("Stratified sampling: {} patients covered, {} observations selected", patientCount, guaranteedSample.size());

        return buildCubeFromSample(cube, guaranteedSample);
    }

    /**
     * Groups observations by patient ID.
     */
    private <V extends Comparable<V>> Map<Integer, List<KeyAndValue<V>>> groupByPatient(PhenoCube<V> cube) {
        Map<Integer, List<KeyAndValue<V>>> byPatient = new HashMap<>();

        for (KeyAndValue<V> kv : cube.sortedByKey()) {
            byPatient.computeIfAbsent(kv.getKey(), k -> new ArrayList<>()).add(kv);
        }

        return byPatient;
    }

    /**
     * Builds a new PhenoCube from sampled observations.
     */
    @SuppressWarnings("unchecked")
    private <V extends Comparable<V>> PhenoCube<V> buildCubeFromSample(PhenoCube<V> original, List<KeyAndValue<V>> sampled) {
        PhenoCube<V> degraded = new PhenoCube<>(original.name, original.vType);
        degraded.setColumnWidth(original.getColumnWidth());
        degraded.setLoadingMap(sampled);
        return degraded;
    }

    /**
     * Counts unique patient IDs in the cube.
     */
    private <V extends Comparable<V>> int countUniquePatients(PhenoCube<V> cube) {
        return Arrays.stream(cube.sortedByKey())
                .map(KeyAndValue::getKey)
                .collect(Collectors.toSet())
                .size();
    }
}

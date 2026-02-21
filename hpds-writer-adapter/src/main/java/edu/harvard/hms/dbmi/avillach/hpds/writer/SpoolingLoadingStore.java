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
import java.util.concurrent.*;
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
    private final int finalizationConcurrency;
    private final int finalizationChunkSize;
    private final boolean disableAdaptiveDegradation;

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

    public SpoolingLoadingStore(Path spoolDirectory, String outputDirectory, String encryptionKeyName, int cacheSize, int maxObservationsPerConcept, int finalizationConcurrency, int finalizationChunkSize, boolean disableAdaptiveDegradation) {
        this.spoolDirectory = spoolDirectory;
        // Ensure outputDirectory ends with /
        this.outputDirectory = outputDirectory.endsWith("/") ? outputDirectory : outputDirectory + "/";
        this.encryptionKeyName = encryptionKeyName;
        this.cacheSize = cacheSize;
        this.maxObservationsPerConcept = maxObservationsPerConcept;
        this.finalizationConcurrency = finalizationConcurrency;
        this.finalizationChunkSize = finalizationChunkSize;
        this.disableAdaptiveDegradation = disableAdaptiveDegradation;

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
            5_000_000,  // Default: 5M observations per concept (~190MB)
            12,         // Default: 12 concurrent finalizations
            1000,       // Default: 1000 concepts per chunk
            false       // Default: adaptive degradation enabled
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
     *
     * Uses hierarchical directory structure (2-level hash) to avoid filesystem limits with 200K+ concepts.
     */
    private void spoolPartial(String conceptPath, PhenoCube cube) {
        try {
            ConceptMetadata meta = conceptMetadata.get(conceptPath);
            if (meta == null) {
                throw new IllegalStateException("No metadata for concept: " + conceptPath);
            }

            // Create hierarchical spool file path
            int partialNum = meta.totalPartialCount++;
            Path spoolFile = getSpoolFilePath(conceptPath, partialNum);

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
     * Generates hierarchical spool file path using 2-level hash-based directory structure.
     *
     * For 200K+ concepts with 7 partials each = 1.4M files:
     * - Flat structure: 1.4M files in single directory → filesystem degradation
     * - Hierarchical structure: 256 × 256 subdirectories → max ~65K files per directory
     *
     * Path format: /spool/AB/CD/full_hash.partial.N.gz
     *
     * @param conceptPath The concept path
     * @param partialNum The partial number
     * @return Path to the spool file
     * @throws IOException if directory creation fails
     */
    private Path getSpoolFilePath(String conceptPath, int partialNum) throws IOException {
        String hash = hashConceptPath(conceptPath);

        // Use first 4 hex chars for 2-level bucketing (256 × 256 = 65,536 buckets)
        String bucket1 = hash.substring(0, 2);  // First 2 hex chars (256 buckets)
        String bucket2 = hash.substring(2, 4);  // Next 2 hex chars (256 subdirs each)

        // Create directory structure: /spool/AB/CD/
        Path bucketDir = spoolDirectory.resolve(bucket1).resolve(bucket2);
        Files.createDirectories(bucketDir);

        // File name: full_hash.partial.N.gz
        String fileName = hash + ".partial." + partialNum + ".gz";
        return bucketDir.resolve(fileName);
    }

    /**
     * Generates SHA-256 hash of concept path for hierarchical directory bucketing.
     *
     * @param conceptPath The concept path to hash
     * @return Hex string representation of SHA-256 hash
     */
    private String hashConceptPath(String conceptPath) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = digest.digest(conceptPath.getBytes(StandardCharsets.UTF_8));

            // Convert to hex string
            StringBuilder hexString = new StringBuilder(hashBytes.length * 2);
            for (byte b : hashBytes) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) {
                    hexString.append('0');
                }
                hexString.append(hex);
            }
            return hexString.toString();

        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    }

    /**
     * Finalizes all concepts and writes to allObservationsStore.
     * This is where each concept is written exactly once.
     *
     * For massive scale (200K+ concepts), processes concepts in chunks to:
     * - Control memory usage (GC between chunks)
     * - Provide progress visibility
     * - Enable recovery from partial failures
     */
    public void saveStore() throws IOException {
        log.info("Finalizing store: flushing cache and merging spooled partials");

        // Flush cache (spools remaining concepts)
        cache.invalidateAll();
        cache.cleanUp();

        // Prepare for chunked processing
        ConcurrentHashMap<String, ColumnMeta> metadataMap = new ConcurrentHashMap<>();
        List<String> failedConcepts = Collections.synchronizedList(new ArrayList<>());
        Map<String, String> failureDetails = new ConcurrentHashMap<>(); // conceptPath -> error message
        List<String> allConcepts = new ArrayList<>(conceptMetadata.keySet());
        int totalConcepts = allConcepts.size();
        int totalChunks = (totalConcepts + finalizationChunkSize - 1) / finalizationChunkSize;

        long finalizationStart = System.currentTimeMillis();
        Runtime runtime = Runtime.getRuntime();
        long heapBeforeMB = (runtime.totalMemory() - runtime.freeMemory()) / 1024 / 1024;

        log.info("=====Finalizing concepts in CHUNKED parallel mode=====");
        log.info("Total concepts:       {}", totalConcepts);
        log.info("Chunk size:           {}", finalizationChunkSize);
        log.info("Total chunks:         {}", totalChunks);
        log.info("Concurrency/chunk:    {}", finalizationConcurrency);
        log.info("Heap before:          {} MB / {} MB max", heapBeforeMB, runtime.maxMemory() / 1024 / 1024);

        try (RandomAccessFile allObservationsStore = new RandomAccessFile(outputDirectory + "allObservationsStore.javabin", "rw")) {

            // Process concepts in chunks
            for (int chunkIdx = 0; chunkIdx < totalChunks; chunkIdx++) {
                int startIdx = chunkIdx * finalizationChunkSize;
                int endIdx = Math.min(startIdx + finalizationChunkSize, totalConcepts);
                List<String> chunk = allConcepts.subList(startIdx, endIdx);

                long chunkStart = System.currentTimeMillis();
                long heapBeforeChunkMB = (runtime.totalMemory() - runtime.freeMemory()) / 1024 / 1024;

                log.debug("===== Processing chunk {}/{}: concepts {}-{} ({} concepts) =====",
                         chunkIdx + 1, totalChunks, startIdx + 1, endIdx, chunk.size());

                // Process this chunk in parallel
                processChunk(chunk, allObservationsStore, metadataMap, failedConcepts, failureDetails);

                long chunkEnd = System.currentTimeMillis();
                long chunkTime = chunkEnd - chunkStart;
                long heapAfterChunkMB = (runtime.totalMemory() - runtime.freeMemory()) / 1024 / 1024;

                log.debug("Chunk {}/{} complete: {}ms | Heap: {} MB -> {} MB (delta: {} MB) | Concepts finalized: {}/{}",
                         chunkIdx + 1, totalChunks, chunkTime,
                         heapBeforeChunkMB, heapAfterChunkMB, heapAfterChunkMB - heapBeforeChunkMB,
                         metadataMap.size(), totalConcepts);

                // Force GC between chunks to reclaim memory
                if (chunkIdx < totalChunks - 1) {
                    log.debug("Forcing GC between chunks to reclaim memory");
                    System.gc();
                    long heapAfterGCMB = (runtime.totalMemory() - runtime.freeMemory()) / 1024 / 1024;
                    log.debug("Heap after GC: {} MB (reclaimed: {} MB)", heapAfterGCMB, heapAfterChunkMB - heapAfterGCMB);
                }
            }

            long finalizationEnd = System.currentTimeMillis();
            long totalTime = finalizationEnd - finalizationStart;
            long heapAfterMB = (runtime.totalMemory() - runtime.freeMemory()) / 1024 / 1024;

            log.info("========== FINALIZATION SUMMARY ==========");
            log.info("Total time:           {}ms ({} seconds)", totalTime, totalTime / 1000.0);
            log.info("Concepts finalized:   {}", metadataMap.size());
            log.info("Chunks processed:     {}", totalChunks);
            log.info("Concurrency/chunk:    {}", finalizationConcurrency);
            log.info("Avg per concept:      {}ms", String.format("%.1f", totalTime / (double) totalConcepts));
            log.info("Avg per chunk:        {}ms", String.format("%.1f", totalTime / (double) totalChunks));
            log.info("Heap before/after:    {} MB -> {} MB (delta: {} MB)", heapBeforeMB, heapAfterMB, heapAfterMB - heapBeforeMB);
            log.info("==========================================");

            // Report failures as warnings (don't abort - we finalized the successful concepts)
            if (!failedConcepts.isEmpty()) {
                log.warn("========== FINALIZATION WARNINGS ==========");

                log.warn("Successfully finalized {}/{} concepts ({}%)",
                        metadataMap.size(), totalConcepts,
                        String.format("%.2f", 100.0 * metadataMap.size() / (double) totalConcepts));

                log.warn("Failed to finalize {} concepts ({}%):",
                        failedConcepts.size(),
                        String.format("%.2f", 100.0 * failedConcepts.size() / (double) totalConcepts));

                // Log each failure with its detailed error
                for (String conceptPath : failedConcepts) {
                    String errorDetail = failureDetails.getOrDefault(conceptPath, "Unknown error");
                    log.warn("  - {} | Reason: {}", conceptPath, errorDetail);
                }

                log.warn("==========================================");
                log.warn("IMPORTANT: {} successful concepts WERE written to allObservationsStore.javabin", metadataMap.size());
                log.warn("Failed concepts will NOT be queryable in HPDS");
                log.warn("Review the errors above to determine if intervention is needed");
            }
        }

        // Write columnMeta.javabin (for successful concepts)
        // Convert ConcurrentHashMap to TreeMap for compatibility with existing readers
        log.info("Writing columnMeta.javabin");
        TreeMap<String, ColumnMeta> sortedMetadata = new TreeMap<>(metadataMap);
        try (ObjectOutputStream metaOut = new ObjectOutputStream(
            new GZIPOutputStream(new FileOutputStream(outputDirectory + "columnMeta.javabin")))) {
            metaOut.writeObject(sortedMetadata);
            metaOut.writeObject(allIds);
        }

        // Write columnMeta.csv for data dictionary using shared utility
        edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.util.ColumnMetaBuilder
            .writeColumnMetaCsv(sortedMetadata, Paths.get(outputDirectory + "columnMeta.csv"));

        // Cleanup spool files
        cleanupSpool();

        log.info("Store finalized: {} concepts, {} patients", metadataMap.size(), allIds.size());
    }

    /**
     * Processes a chunk of concepts in parallel using bounded virtual threads.
     *
     * @param chunk List of concept paths to process
     * @param store RandomAccessFile for writing
     * @param metadataMap Concurrent map for column metadata
     * @param failedConcepts List to collect failures
     * @param failureDetails Map to collect failure reasons
     */
    private void processChunk(List<String> chunk,
                             RandomAccessFile store,
                             ConcurrentHashMap<String, ColumnMeta> metadataMap,
                             List<String> failedConcepts,
                             Map<String, String> failureDetails) {
        ExecutorService executor = Executors.newFixedThreadPool(finalizationConcurrency, Thread.ofVirtual().factory());
        List<Future<?>> futures = new ArrayList<>();

        for (String conceptPath : chunk) {
            futures.add(executor.submit(() -> {
                try {
                    finalizeConceptParallel(conceptPath, store, metadataMap);
                } catch (Exception e) {
                    // Extract concise error message for reporting
                    String errorMsg = e.getClass().getSimpleName() + ": " + e.getMessage();
                    if (e.getCause() != null) {
                        errorMsg += " (caused by: " + e.getCause().getClass().getSimpleName() + ")";
                    }

                    log.error("Failed to finalize concept: {} | Error: {}", conceptPath, errorMsg, e);
                    failedConcepts.add(conceptPath);
                    failureDetails.put(conceptPath, errorMsg);
                    // Don't throw - let other concepts finish
                }
            }));
        }

        // Wait for all concepts in this chunk to finish
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                log.error("Future execution failed", e);
            }
        }

        executor.shutdown();
        try {
            if (!executor.awaitTermination(30, TimeUnit.MINUTES)) {
                log.warn("Chunk finalization did not complete within 30 minutes");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.error("Interrupted while waiting for chunk finalization", e);
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Finalizes a single concept in parallel: merge partials, complete, and write to store.
     * This method is called by virtual threads for parallel processing.
     *
     * @param conceptPath The concept path to finalize
     * @param store The RandomAccessFile for allObservationsStore
     * @param metadataMap Concurrent map to store column metadata
     * @throws IOException if finalization fails
     */
    private void finalizeConceptParallel(String conceptPath,
                                        RandomAccessFile store,
                                        ConcurrentHashMap<String, ColumnMeta> metadataMap) throws IOException {
        long conceptStart = System.currentTimeMillis();
        log.debug("Finalizing concept in parallel: {} (thread: {})", conceptPath, Thread.currentThread().getName());

        ConceptMetadata meta = conceptMetadata.get(conceptPath);
        PhenoCube finalCube = mergePartials(conceptPath, meta);
        long mergeEnd = System.currentTimeMillis();

        complete(finalCube);
        long completeEnd = System.currentTimeMillis();

        // Synchronize only the file write (not the entire finalization)
        ColumnMeta columnMeta;
        synchronized (store) {
            columnMeta = writeToStore(store, conceptPath, finalCube, meta);
        }
        long writeEnd = System.currentTimeMillis();

        metadataMap.put(conceptPath, columnMeta);

        long totalTime = writeEnd - conceptStart;
        log.info("Finalized concept: {} | Total: {}ms | Merge: {}ms | Complete: {}ms | Write: {}ms | Observations: {}",
                 conceptPath,
                 totalTime,
                 mergeEnd - conceptStart,
                 completeEnd - mergeEnd,
                 writeEnd - completeEnd,
                 finalCube.sortedByKey().length);
    }

    /**
     * Merges all spooled partials for a concept into a single PhenoCube.
     *
     * DEDUPLICATION: During merge, filters out duplicate observations using fingerprint matching.
     * This ensures each unique observation (patientNum, conceptPath, value, timestamp) appears
     * exactly once, even if the same observation was ingested multiple times from different sources.
     *
     * Memory bounded: HashSet<ObservationFingerprint> is cleared after each concept.
     * Worst case: 50M observations × 29 bytes/fingerprint = ~1.45GB per concept.
     */
    @SuppressWarnings("unchecked")
    private PhenoCube mergePartials(String conceptPath, ConceptMetadata meta) throws IOException {
        List<Path> partials = spoolFiles.getOrDefault(conceptPath, Collections.emptyList());

        Class<?> valueType = meta.isCategorical ? String.class : Double.class;
        PhenoCube merged = new PhenoCube(conceptPath, valueType);
        merged.setColumnWidth(meta.columnWidth);

        // SAFETY CHECK: Prevent Java array size limit violation (Integer.MAX_VALUE = 2,147,483,647)
        // Conservative estimate: assume each partial has maxObservationsPerConcept observations
        long estimatedTotalObs = (long) partials.size() * maxObservationsPerConcept;
        final int JAVA_ARRAY_LIMIT = Integer.MAX_VALUE - 10_000_000; // 2,137,483,647 with 10M safety buffer

        if (estimatedTotalObs > JAVA_ARRAY_LIMIT) {
            log.error("═══════════════════════════════════════════════════════════════");
            log.error("CRITICAL: Concept '{}' EXCEEDS JAVA ARRAY LIMIT", conceptPath);
            log.error("═══════════════════════════════════════════════════════════════");
            log.error("Partial files found:     {}", partials.size());
            log.atError().setMessage("Estimated observations:  {} ({} B)")
                    .addArgument(estimatedTotalObs)
                    .addArgument(() -> String.format("%.2f", estimatedTotalObs / 1_000_000_000.0))
                    .log();
            log.atError().setMessage("Java array limit:        {} ({} B)")
                    .addArgument(JAVA_ARRAY_LIMIT)
                    .addArgument(() -> String.format("%.2f", JAVA_ARRAY_LIMIT / 1_000_000_000.0))
                    .log();
            long overflow = estimatedTotalObs - JAVA_ARRAY_LIMIT;
            double overflowPct = 100.0 * overflow / JAVA_ARRAY_LIMIT;
            log.atError().setMessage("Overflow:                {} observations ({} over limit)")
                    .addArgument(overflow)
                    .addArgument(String.format("%.1f%%", overflowPct))
                    .log();
            log.error("");
            log.error("This concept cannot be finalized without data loss.");
            log.error("");
            log.error("SOLUTION: Reduce max-observations-per-concept to 5M");
            log.error("  - Current setting: {}M observations per partial", maxObservationsPerConcept / 1_000_000);
            log.error("  - Allows {} partials before hitting limit", partials.size());
            log.error("  - Recommended: ingest.max-observations-per-concept=5000000");
            log.error("  - This allows: 2.1B / 5M = 420 max partials (safe)");
            log.error("═══════════════════════════════════════════════════════════════");

            throw new IOException(String.format(
                "Concept '%s' exceeds Java array limit: %d partials × %dM ≈ %.2fB observations > %.2fB limit (Integer.MAX_VALUE). " +
                "Cannot merge without data loss. REQUIRED ACTION: Reduce ingest.max-observations-per-concept to 5000000 (5M) and re-ingest.",
                conceptPath,
                partials.size(),
                maxObservationsPerConcept / 1_000_000,
                estimatedTotalObs / 1_000_000_000.0,
                JAVA_ARRAY_LIMIT / 1_000_000_000.0));
        }

        // ========== DEDUPLICATION LOGIC ==========
        // Track unique observations for THIS concept only
        Set<ObservationFingerprint> seenInThisConcept = new HashSet<>();
        List<KeyAndValue> allEntries = new ArrayList<>();

        int totalRead = 0;
        int duplicatesSkipped = 0;
        long dedupStart = System.currentTimeMillis();

        // Read all partials
        for (Path partial : partials) {
            try (InputStream fis = Files.newInputStream(partial);
                 GZIPInputStream gzis = new GZIPInputStream(fis);
                 ObjectInputStream ois = new ObjectInputStream(gzis)) {
                List<KeyAndValue> entries = (List<KeyAndValue>) ois.readObject();
                totalRead += entries.size();

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

                // Deduplicate: check fingerprint before adding
                for (KeyAndValue entry : entries) {
                    ObservationFingerprint fp = new ObservationFingerprint(
                        entry.getKey(),                                    // patientNum
                        conceptPath,                                       // conceptPath
                        entry.getValue().toString(),                       // value (normalized)
                        entry.getTimestamp() != null ? entry.getTimestamp() : 0L  // timestamp
                    );

                    if (seenInThisConcept.add(fp)) {
                        // Unique observation - add to final list
                        allEntries.add(entry);
                    } else {
                        // Duplicate observation - skip
                        duplicatesSkipped++;
                    }
                }
            } catch (ClassNotFoundException e) {
                throw new IOException("Failed to deserialize partial: " + partial, e);
            }
        }

        // Free deduplication memory immediately
        seenInThisConcept.clear();

        long dedupEnd = System.currentTimeMillis();
        long dedupTime = dedupEnd - dedupStart;

        // Log deduplication statistics
        if (duplicatesSkipped > 0) {
            double dedupRate = 100.0 * duplicatesSkipped / totalRead;
            log.atInfo().setMessage("Dedup for '{}': {} read, {} unique, {} duplicates ({}) | Time: {}ms")
                    .addArgument(conceptPath)
                    .addArgument(totalRead)
                    .addArgument(allEntries.size())
                    .addArgument(duplicatesSkipped)
                    .addArgument(String.format("%.1f%%", dedupRate))
                    .addArgument(dedupTime)
                    .log();
        } else {
            log.debug("Dedup for '{}': {} observations, no duplicates | Time: {}ms",
                      conceptPath, totalRead, dedupTime);
        }

        merged.setLoadingMap(allEntries);
        log.debug("Merged {} partials for concept {} ({} unique observations after dedup)",
            partials.size(), conceptPath, allEntries.size());

        return merged;
    }

    /**
     * Completes a cube: sorts by key, builds category maps, computes min/max.
     * This logic is extracted from the original LoadingStore.
     *
     * Optimized for massive scale (200K+ concepts):
     * - Direct array operations instead of Stream API (saves 10 hours at 200K scale)
     * - Eliminates intermediate ArrayList, Stream, and Collector objects
     */
    @SuppressWarnings("unchecked")
    private <V extends Comparable<V>> void complete(PhenoCube<V> cube) {
        List<KeyAndValue<V>> loadingMap = cube.getLoadingMap();
        if (loadingMap == null || loadingMap.isEmpty()) {
            log.warn("Empty loading map for concept: {}", cube.name);
            cube.setSortedByKey(new KeyAndValue[0]);
            return;
        }

        // Sort by key - OPTIMIZED: Direct array operations (no Stream API overhead)
        KeyAndValue<V>[] sortedArray = loadingMap.toArray(new KeyAndValue[loadingMap.size()]);
        Arrays.sort(sortedArray, Comparator.comparingInt(KeyAndValue::getKey));
        cube.setSortedByKey(sortedArray);

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
     * Estimates serialized size of a PhenoCube to pre-size ByteArrayOutputStream.
     *
     * Avoids expensive reallocations (32 → 64 → 128 → ... → 1.5GB).
     * Each reallocation copies the entire array, causing 500ms overhead per large concept.
     *
     * Estimation formula (conservative):
     * - KeyAndValue array overhead: 80 bytes per observation
     * - Value overhead: 8 bytes (Double) or 50 bytes average (String)
     * - ObjectOutputStream overhead: ~10% of data
     * - Add 20% safety buffer
     *
     * @param cube The cube to estimate
     * @return Estimated serialization size in bytes
     */
    private int estimateSerializationSize(PhenoCube<?> cube) {
        int observationCount = cube.sortedByKey().length;
        if (observationCount == 0) {
            return 1024; // Minimum buffer
        }

        // Base overhead: 80 bytes per KeyAndValue entry
        long baseSize = observationCount * 80L;

        // Value overhead: 8 bytes for Double, ~50 bytes for String
        long valueSize = cube.isStringType()
                ? observationCount * 50L  // String (conservative estimate)
                : observationCount * 8L;  // Double

        // ObjectOutputStream overhead: ~10%
        long totalEstimate = (long) ((baseSize + valueSize) * 1.10);

        // Add 20% safety buffer to reduce likelihood of reallocation
        totalEstimate = (long) (totalEstimate * 1.20);

        // Clamp to reasonable bounds
        if (totalEstimate > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE; // ByteArrayOutputStream max size
        }
        if (totalEstimate < 4096) {
            return 4096; // Minimum reasonable buffer
        }

        return (int) totalEstimate;
    }

    /**
     * Writes a finalized cube to allObservationsStore and returns ColumnMeta.
     * Includes adaptive degradation to prevent 2GB serialization failures.
     */
    @SuppressWarnings("unchecked")
    private ColumnMeta writeToStore(RandomAccessFile store, String conceptPath, PhenoCube cube, ConceptMetadata meta) throws IOException {
        // STEP 1: Attempt adaptive degradation if needed (unless disabled)
        PhenoCube finalCube = cube;

        if (!disableAdaptiveDegradation) {
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
        } else {
            log.debug("Adaptive degradation disabled - writing cube as-is for: {}", conceptPath);
        }

        // STEP 2: Build ColumnMeta using shared utility
        ColumnMeta columnMeta = edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.util.ColumnMetaBuilder
            .fromPhenoCube(finalCube, meta.columnWidth);
        columnMeta.setAllObservationsOffset(store.getFilePointer());

        // STEP 3: Serialize and encrypt with size validation
        // OPTIMIZED: Pre-size ByteArrayOutputStream to avoid reallocations (saves 27.7 hours at 200K scale)
        int estimatedSize = estimateSerializationSize(finalCube);
        try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream(estimatedSize);
             ObjectOutputStream out = new ObjectOutputStream(byteStream)) {
            out.writeObject(finalCube);
            out.flush();

            byte[] serialized = byteStream.toByteArray();
            double accuracy = 100.0 * estimatedSize / serialized.length;
            log.atDebug().setMessage("Serialized {} to {} bytes (estimated: {}, accuracy: {})")
                    .addArgument(conceptPath)
                    .addArgument(serialized.length)
                    .addArgument(estimatedSize)
                    .addArgument(String.format("%.1f%%", accuracy))
                    .log();

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
    /**
     * Adaptive degradation: Reduces concept observations to fit within 2GB encryption limit.
     *
     * DEPRECATED: This method is superseded by max-observations-per-concept enforcement
     * during ingestion, which prevents concepts from exceeding limits in the first place.
     *
     * The complex sampling/verification logic here adds ~8 seconds per concept overhead.
     * At 200K scale, this would add 18.5 DAYS to finalization time.
     *
     * With max-observations-per-concept=10M enforced during ingestion, concepts never
     * reach the 2GB limit, making this degradation logic redundant.
     *
     * This stub implementation immediately returns the cube unchanged, eliminating
     * the 18.5-day overhead at massive scale.
     *
     * @deprecated Use max-observations-per-concept config instead (enforced during ingestion)
     */
    @Deprecated
    private <V extends Comparable<V>> PhenoCube<V> adaptiveDegradation(String conceptPath,
                                                                        PhenoCube<V> cube,
                                                                        ConceptMetadata meta) throws IOException {
        // Stub: Return cube unchanged
        // Degradation is now handled upstream by max-observations-per-concept limit during ingestion
        log.debug("Adaptive degradation bypassed for {} ({} obs) - using max-observations limit instead",
                  conceptPath, cube.sortedByKey().length);
        return cube;
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

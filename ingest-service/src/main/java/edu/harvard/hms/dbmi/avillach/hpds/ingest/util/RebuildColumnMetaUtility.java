package edu.harvard.hms.dbmi.avillach.hpds.ingest.util;

import edu.harvard.hms.dbmi.avillach.hpds.crypto.Crypto;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.ColumnMeta;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.KeyAndValue;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.PhenoCube;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

/**
 * Utility to rebuild columnMeta.javabin and columnMeta.csv from allObservationsStore.javabin.
 *
 * <p>Use cases:
 * <ul>
 *   <li>Fix corrupted columnMeta.javabin</li>
 *   <li>Rebuild metadata from scratch if only allObservationsStore exists</li>
 *   <li>Validate metadata consistency</li>
 * </ul>
 *
 * <p>Usage:
 * <pre>
 * java -cp ingest-service.jar \
 *   edu.harvard.hms.dbmi.avillach.hpds.ingest.util.RebuildColumnMetaUtility \
 *   &lt;inputDir&gt; &lt;outputDir&gt; [--encryption-key-name &lt;name&gt;] [--key-file &lt;path&gt;]
 * </pre>
 *
 * <p>Example:
 * <pre>
 * java -cp ingest-service.jar \
 *   edu.harvard.hms.dbmi.avillach.hpds.ingest.util.RebuildColumnMetaUtility \
 *   /path/to/input /path/to/output
 * </pre>
 */
public class RebuildColumnMetaUtility {
    private static final Logger log = LoggerFactory.getLogger(RebuildColumnMetaUtility.class);

    public static void main(String[] args) {
        if (args.length < 2) {
            printUsage();
            System.exit(1);
        }

        String inputDir = args[0];
        String outputDir = args[1];
        String encryptionKeyName = "encryption_key"; // Default
        String keyFilePath = null;

        // Parse optional arguments
        for (int i = 2; i < args.length; i++) {
            if ("--encryption-key-name".equals(args[i]) && i + 1 < args.length) {
                encryptionKeyName = args[++i];
            } else if ("--key-file".equals(args[i]) && i + 1 < args.length) {
                keyFilePath = args[++i];
            }
        }

        log.info("=== Rebuild ColumnMeta Utility ===");
        log.info("Input directory:  {}", inputDir);
        log.info("Output directory: {}", outputDir);
        log.info("Encryption key:   {}", encryptionKeyName);

        try {
            // Load encryption key
            if (keyFilePath != null) {
                log.info("Loading encryption key from: {}", keyFilePath);
                Crypto.loadKey(encryptionKeyName, Paths.get(keyFilePath).toString());
            } else {
                // Try to find key in standard locations
                Path defaultKeyPath = Paths.get(outputDir, encryptionKeyName);
                if (Files.exists(defaultKeyPath)) {
                    log.info("Loading encryption key from: {}", defaultKeyPath);
                    Crypto.loadKey(encryptionKeyName, defaultKeyPath.toString());
                } else {
                    log.warn("Encryption key not found at {}. Attempting to use already-loaded key.", defaultKeyPath);
                }
            }

            // Rebuild metadata
            RebuildColumnMetaUtility utility = new RebuildColumnMetaUtility(
                inputDir,
                outputDir,
                encryptionKeyName
            );

            utility.rebuild();

            log.info("✓ Rebuild complete!");
            System.exit(0);

        } catch (Exception e) {
            log.error("✗ Rebuild failed", e);
            System.exit(1);
        }
    }

    private static void printUsage() {
        System.out.println("Usage: RebuildColumnMetaUtility <inputDir> <outputDir> [options]");
        System.out.println();
        System.out.println("Required arguments:");
        System.out.println("  <inputDir>   Directory containing allObservationsStore.javabin");
        System.out.println("  <outputDir>  Directory where columnMeta.javabin and .csv will be written");
        System.out.println();
        System.out.println("Optional arguments:");
        System.out.println("  --encryption-key-name <name>  Name of encryption key (default: encryption_key)");
        System.out.println("  --key-file <path>             Path to encryption key file");
        System.out.println();
        System.out.println("Example:");
        System.out.println("  java -cp ingest-service.jar \\");
        System.out.println("    edu.harvard.hms.dbmi.avillach.hpds.ingest.util.RebuildColumnMetaUtility \\");
        System.out.println("    /path/to/input /path/to/output");
    }

    private final String inputDir;
    private final String outputDir;
    private final String encryptionKeyName;

    public RebuildColumnMetaUtility(String inputDir, String outputDir, String encryptionKeyName) {
        this.inputDir = inputDir;
        this.outputDir = outputDir;
        this.encryptionKeyName = encryptionKeyName;
    }

    /**
     * Rebuild columnMeta.javabin and columnMeta.csv from allObservationsStore.javabin.
     */
    public void rebuild() throws IOException {
        Path allObsPath = Paths.get(inputDir, "allObservationsStore.javabin");
        Path inputMetaPath = Paths.get(inputDir, "columnMeta.javabin");
        Path outputMetaPath = Paths.get(outputDir, "columnMeta.javabin");
        Path outputCsvPath = Paths.get(outputDir, "columnMeta.csv");

        if (!Files.exists(allObsPath)) {
            throw new FileNotFoundException("allObservationsStore.javabin not found at: " + allObsPath);
        }

        // Ensure output directory exists
        Files.createDirectories(Paths.get(outputDir));

        log.info("Reading allObservationsStore from: {}", allObsPath);
        log.info("Writing columnMeta.javabin to: {}", outputMetaPath);
        log.info("Writing columnMeta.csv to: {}", outputCsvPath);

        // Step 1: Try to load existing columnMeta to get offset/length information
        Map<String, ColumnMeta> existingMeta = null;
        TreeSet<Integer> existingAllIds = null;
        if (Files.exists(inputMetaPath)) {
            log.info("Found existing columnMeta.javabin at: {}", inputMetaPath);
            try (ObjectInputStream ois = new ObjectInputStream(
                    new java.util.zip.GZIPInputStream(new FileInputStream(inputMetaPath.toFile())))) {
                Object firstObj = ois.readObject();
                if (firstObj instanceof Map) {
                    existingMeta = (Map<String, ColumnMeta>) firstObj;
                    log.info("Loaded {} concept metadata entries (type: {})",
                            existingMeta.size(), firstObj.getClass().getSimpleName());
                }
                // Try to read the second object (allIds TreeSet)
                try {
                    Object secondObj = ois.readObject();
                    if (secondObj instanceof TreeSet) {
                        existingAllIds = (TreeSet<Integer>) secondObj;
                        log.info("Loaded {} patient IDs from existing metadata", existingAllIds.size());
                    }
                } catch (Exception e) {
                    log.warn("Could not load allIds from existing metadata: {}", e.getMessage());
                }
            } catch (Exception e) {
                log.warn("Failed to load existing columnMeta (will scan file sequentially): {}", e.getMessage());
                existingMeta = null;
            }
        }

        // Step 2: Rebuild metadata using offset/length from existing meta
        TreeMap<String, ColumnMeta> metadataMap = new TreeMap<>();
        TreeSet<Integer> allIds = new TreeSet<>();

        int conceptCount = 0;
        int errorCount = 0;

        if (existingMeta != null && !existingMeta.isEmpty()) {
            // Parallel processing: decrypt/deserialize each concept to rebuild metadata and extract patient IDs
            log.info("Processing {} concepts in parallel to rebuild metadata and extract patient IDs", existingMeta.size());

            int parallelism = Runtime.getRuntime().availableProcessors();
            log.info("Using {} threads for parallel processing", parallelism);

            // Use memory-mapped file for parallel random access (handles files > 2GB)
            long fileSize = Files.size(allObsPath);
            log.info("File size: {} GB", String.format("%.2f", fileSize / 1_073_741_824.0));

            // Thread-safe collections
            ConcurrentHashMap<String, ColumnMeta> concurrentMetadataMap = new ConcurrentHashMap<>();
            ConcurrentSkipListSet<Integer> concurrentAllIds = new ConcurrentSkipListSet<>();
            AtomicInteger processed = new AtomicInteger(0);
            AtomicInteger errors = new AtomicInteger(0);

            // Process concepts in parallel
            List<Map.Entry<String, ColumnMeta>> entries = new ArrayList<>(existingMeta.entrySet());
            final int totalConcepts = existingMeta.size();

            try (RandomAccessFile raf = new RandomAccessFile(allObsPath.toFile(), "r");
                 FileChannel channel = raf.getChannel();
                 ExecutorService executor = Executors.newFixedThreadPool(parallelism)) {

                List<Future<?>> futures = new ArrayList<>();

                for (Map.Entry<String, ColumnMeta> entry : entries) {
                    futures.add(executor.submit(() -> {
                        try {
                            ColumnMeta oldMeta = entry.getValue();
                            long startOffset = oldMeta.getAllObservationsOffset();
                            long endOffset = oldMeta.getAllObservationsLength();
                            int length = (int) (endOffset - startOffset);

                            // Read bytes from file using synchronized access
                            byte[] encrypted = new byte[length];
                            synchronized (channel) {
                                ByteBuffer buffer = ByteBuffer.wrap(encrypted);
                                channel.position(startOffset);
                                channel.read(buffer);
                            }

                            // Decrypt and deserialize
                            byte[] decrypted = Crypto.decryptData(encryptionKeyName, encrypted);
                            try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(decrypted))) {
                                PhenoCube<?> cube = (PhenoCube<?>) ois.readObject();

                                // Build fresh ColumnMeta from PhenoCube
                                ColumnMeta meta = buildColumnMeta(cube, startOffset, endOffset);
                                concurrentMetadataMap.put(meta.getName(), meta);

                                // Collect all patient IDs
                                for (KeyAndValue<?> kv : cube.sortedByKey()) {
                                    concurrentAllIds.add(kv.getKey());
                                }
                            }

                            int count = processed.incrementAndGet();
                            if (count % 1000 == 0) {
                                double percentComplete = 100.0 * count / totalConcepts;
                                log.atInfo().setMessage("Progress: {} concepts processed ({} complete)")
                                        .addArgument(count)
                                        .addArgument(String.format("%.1f%%", percentComplete))
                                        .log();
                            }

                        } catch (Exception e) {
                            log.error("Failed to process concept {}: {}", entry.getKey(), e.getMessage());
                            errors.incrementAndGet();
                        }
                    }));
                }

                // Wait for all tasks to complete
                executor.shutdown();
                try {
                    executor.awaitTermination(1, TimeUnit.HOURS);

                    // Check for failures
                    for (Future<?> future : futures) {
                        future.get(); // Propagate any exceptions
                    }
                } catch (InterruptedException | ExecutionException e) {
                    log.error("Error during parallel processing: {}", e.getMessage());
                    throw new IOException("Parallel processing failed", e);
                }
            }

            // Convert to TreeMap and TreeSet
            metadataMap.putAll(concurrentMetadataMap);
            allIds.addAll(concurrentAllIds);

            conceptCount = metadataMap.size();
            errorCount = errors.get();

            log.info("Parallel processing complete: {} concepts, {} errors, {} patient IDs",
                    conceptCount, errorCount, allIds.size());
        } else {
            log.error("Cannot rebuild without existing columnMeta.javabin - file has no length prefixes");
            throw new IllegalStateException(
                "allObservationsStore.javabin does not contain length prefixes. " +
                "An existing columnMeta.javabin file is required to determine concept boundaries. " +
                "Please provide columnMeta.javabin in the input directory."
            );
        }

        // Step 2: Write TreeMap to columnMeta.javabin
        log.info("Writing columnMeta.javabin...");
        try (ObjectOutputStream metaOut = new ObjectOutputStream(
                new GZIPOutputStream(new FileOutputStream(outputMetaPath.toFile())))) {
            metaOut.writeObject(metadataMap);
            metaOut.writeObject(allIds);
        }
        log.info("✓ Wrote columnMeta.javabin ({} concepts)", metadataMap.size());

        // Step 3: Write columnMeta.csv
        log.info("Writing columnMeta.csv...");
        writeColumnMetaCsv(metadataMap, outputCsvPath);
        log.info("✓ Wrote columnMeta.csv ({} rows)", metadataMap.size());
    }

    /**
     * Build ColumnMeta from PhenoCube.
     */
    @SuppressWarnings("unchecked")
    private ColumnMeta buildColumnMeta(PhenoCube<?> cube, long startOffset, long endOffset) {
        String conceptPath = cube.name;
        boolean isCategorical = cube.isStringType();

        ColumnMeta meta = new ColumnMeta()
            .setName(conceptPath)
            .setCategorical(isCategorical)
            .setAllObservationsOffset(startOffset)
            .setAllObservationsLength(endOffset);

        KeyAndValue<?>[] observations = cube.sortedByKey();
        meta.setObservationCount(observations.length);

        // Count unique patients
        Set<Integer> uniquePatients = Arrays.stream(observations)
            .map(KeyAndValue::getKey)
            .collect(Collectors.toSet());
        meta.setPatientCount(uniquePatients.size());

        // Set category values or min/max
        if (isCategorical) {
            // Extract unique categorical values (sorted)
            TreeSet<String> uniqueValues = new TreeSet<>();
            for (Object value : cube.keyBasedArray()) {
                if (value != null) {
                    uniqueValues.add(value.toString());
                }
            }
            List<String> categoryValues = new ArrayList<>(uniqueValues);
            meta.setCategoryValues(categoryValues);

            // Calculate width in bytes (longest category value)
            int maxWidth = categoryValues.stream()
                .mapToInt(String::length)
                .max()
                .orElse(1);
            meta.setWidthInBytes(maxWidth);

        } else {
            // Extract min/max for numeric
            double min = Double.MAX_VALUE;
            double max = Double.MIN_VALUE;
            for (Object value : cube.keyBasedArray()) {
                if (value instanceof Double) {
                    double d = (Double) value;
                    min = Math.min(min, d);
                    max = Math.max(max, d);
                }
            }
            meta.setMin(min == Double.MAX_VALUE ? Double.NaN : min);
            meta.setMax(max == Double.MIN_VALUE ? Double.NaN : max);

            // Numeric width is always 1 (stored as Double)
            meta.setWidthInBytes(1);
        }

        return meta;
    }

    /**
     * Write columnMeta.csv (data dictionary).
     *
     * CSV columns:
     * 1. name
     * 2. widthInBytes
     * 3. columnOffset
     * 4. isCategorical
     * 5. categoryValues (µ-delimited)
     * 6. min
     * 7. max
     * 8. allObservationsOffset
     * 9. allObservationsLength
     * 10. observationCount
     * 11. patientCount
     */
    private void writeColumnMetaCsv(TreeMap<String, ColumnMeta> metadataMap, Path csvPath) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(
                csvPath,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING);
             CSVPrinter printer = new CSVPrinter(writer, CSVFormat.DEFAULT)) {

            for (Map.Entry<String, ColumnMeta> entry : metadataMap.entrySet()) {
                ColumnMeta meta = entry.getValue();
                Object[] row = new Object[11];

                // Build category values string (µ-delimited)
                StringBuilder categoryValuesStr = new StringBuilder();
                if (meta.getCategoryValues() != null && !meta.getCategoryValues().isEmpty()) {
                    AtomicInteger idx = new AtomicInteger(0);
                    meta.getCategoryValues().forEach(value -> {
                        categoryValuesStr.append(value);
                        if (idx.incrementAndGet() < meta.getCategoryValues().size()) {
                            categoryValuesStr.append("µ");
                        }
                    });
                }

                row[0] = meta.getName();
                row[1] = String.valueOf(meta.getWidthInBytes());
                row[2] = String.valueOf(meta.getColumnOffset());
                row[3] = String.valueOf(meta.isCategorical());
                row[4] = categoryValuesStr.toString();
                row[5] = String.valueOf(meta.getMin());
                row[6] = String.valueOf(meta.getMax());
                row[7] = String.valueOf(meta.getAllObservationsOffset());
                row[8] = String.valueOf(meta.getAllObservationsLength());
                row[9] = String.valueOf(meta.getObservationCount());
                row[10] = String.valueOf(meta.getPatientCount());

                printer.printRecord(row);
            }
        }
    }
}

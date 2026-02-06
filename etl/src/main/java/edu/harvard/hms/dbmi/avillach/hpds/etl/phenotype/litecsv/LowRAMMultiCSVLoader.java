package edu.harvard.hms.dbmi.avillach.hpds.etl.phenotype.litecsv;

import edu.harvard.hms.dbmi.avillach.hpds.crypto.Crypto;
import edu.harvard.hms.dbmi.avillach.hpds.etl.phenotype.config.ConfigLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static edu.harvard.hms.dbmi.avillach.hpds.crypto.Crypto.DEFAULT_KEY_NAME;

public class LowRAMMultiCSVLoader {

    private static final Logger log = LoggerFactory.getLogger(LowRAMMultiCSVLoader.class);
    private final String inputDir;
    private final String outputDir;
    private final boolean rollUpVarNames;
    private final double maxChunkSize;
    private final ConfigLoader configLoader;

    public LowRAMMultiCSVLoader(String inputDir, String outputDir, boolean rollUpVarNames, double maxChunkSize, ConfigLoader configLoader) {
        this.inputDir = inputDir == null ? "/opt/local/hpds_input" : inputDir;
        this.outputDir = outputDir == null ? "/opt/local/hpds/" : outputDir;
        this.rollUpVarNames = rollUpVarNames;
        this.maxChunkSize = maxChunkSize;
        this.configLoader = configLoader;
        Crypto.loadDefaultKey();
    }

    /**
     * @deprecated Use the constructor with outputDir parameter instead.
     */
    @Deprecated
    public LowRAMMultiCSVLoader(LowRAMLoadingStore store, LowRAMCSVProcessor processor, String inputDir) {
        this.inputDir = inputDir == null ? "/opt/local/hpds_input" : inputDir;
        this.outputDir = "/opt/local/hpds/";
        this.rollUpVarNames = false;
        this.maxChunkSize = 5D;
        this.configLoader = null;
        Crypto.loadDefaultKey();
    }

    public static void main(String[] args) {
        boolean rollUpVarNames = true;
        double maxChunkSize = 5D;
        for (String arg : args) {
            if (arg.equalsIgnoreCase("NO_ROLLUP")) {
                log.info("Configured to not roll up variable names");
                rollUpVarNames = false;
            }

            if (arg.contains("MAX_CHUNK_SIZE")) {
                String[] parts = arg.split("=");
                if (parts.length == 2) {
                    try {
                        maxChunkSize = Double.parseDouble(parts[1]);
                        log.info("Configured to use a max chunk size of {} GB", maxChunkSize);
                    } catch (NumberFormatException e) {
                       throw new IllegalArgumentException("Invalid max chunk size " + maxChunkSize);
                    }
                }
            }
        }

        String inputDir = "/opt/local/hpds_input";
        String outputDir = "/opt/local/hpds/";
        ConfigLoader configLoader = new ConfigLoader();
        LowRAMMultiCSVLoader loader = new LowRAMMultiCSVLoader(inputDir, outputDir, rollUpVarNames, maxChunkSize, configLoader);
        int exitCode = loader.processCSVsFromHPDSDir(maxChunkSize);
        System.exit(exitCode);
    }

    protected int processCSVsFromHPDSDir(double maxChunkSize) {
        log.info("Looking for files to process. All files must be smaller than {}G", maxChunkSize);
        log.info("Files larger than {}G should be split into a series of CSVs", maxChunkSize);
        try (Stream<Path> input_files = Files.list(Path.of(inputDir))) {
            input_files.map(Path::toFile).filter(File::isFile).peek(f -> log.info("Found file {}", f.getAbsolutePath()))
                .filter(f -> f.getName().endsWith(".csv")).peek(f -> log.info("Confirmed file {} is a .csv", f.getAbsolutePath()))
                .forEach(csvFile -> processOneCSV(csvFile, maxChunkSize));
            return 0;
        } catch (IOException e) {
            log.error("Exception processing files: ", e);
            return 1;
        }
    }

    private void processOneCSV(File csvFile, double maxChunkSize) {
        String shardName = deriveShardName(csvFile.getName());
        String shardDir = outputDir + shardName + "/";

        File shardDirFile = new File(shardDir);
        if (!shardDirFile.exists() && !shardDirFile.mkdirs()) {
            throw new UncheckedIOException(new IOException("Failed to create shard directory: " + shardDir));
        }

        String obsTempFile = shardDir + "allObservationsTemp.javabin";
        String columnMetaFile = shardDir + "columnMeta.javabin";
        String obsPermFile = shardDir + "allObservationsStore.javabin";

        log.info("Processing CSV {} into shard directory {}", csvFile.getName(), shardDir);

        LowRAMLoadingStore store = new LowRAMLoadingStore(obsTempFile, columnMetaFile, obsPermFile, DEFAULT_KEY_NAME);
        LowRAMCSVProcessor processor = configLoader != null
            ? new LowRAMCSVProcessor(store, rollUpVarNames, maxChunkSize, configLoader)
            : new LowRAMCSVProcessor(store, rollUpVarNames, maxChunkSize);

        IngestStatus status = processor.process(csvFile);
        log.info("Finished processing file {}", status);

        try {
            store.saveStore();
        } catch (IOException | ClassNotFoundException e) {
            throw new UncheckedIOException(new IOException("Error saving store for shard " + shardName, e));
        }
    }

    static String deriveShardName(String csvFilename) {
        String name = csvFilename;
        if (name.toLowerCase().endsWith(".csv")) {
            name = name.substring(0, name.length() - 4);
        }
        // Replace any non-alphanumeric characters (except - and _) with _
        name = name.replaceAll("[^a-zA-Z0-9_\\-]", "_");
        return "shard_" + name;
    }
}

package edu.harvard.hms.dbmi.avillach.hpds.etl.phenotype.csv;

import edu.harvard.hms.dbmi.avillach.hpds.crypto.Crypto;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.PhenoCube;
import edu.harvard.hms.dbmi.avillach.hpds.etl.LoadingStore;
import edu.harvard.hms.dbmi.avillach.hpds.etl.phenotype.config.CSVConfig;
import edu.harvard.hms.dbmi.avillach.hpds.etl.phenotype.config.ConfigLoader;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.Date;

@SuppressWarnings({"unchecked", "rawtypes"})
@Service
public class LoaderService {

    private static final Logger log = LoggerFactory.getLogger(LoaderService.class);
    private final LoadingStore store = new LoadingStore();
    private static final ConfigLoader configLoader = new ConfigLoader();

    @Value("${etl.hpds.directory:/opt/local/hpds/}")
    private String hpdsDirectory;

    @Value("${etl.rollup.enabled:true}")
    private boolean rollupEnabled;

    private static boolean DO_VARNAME_ROLLUP = false;


    public void runEtlProcess() throws IOException {
        log.info("Starting ETL process... Rollup Enabled: {}", rollupEnabled);

        store.allObservationsStore = new RandomAccessFile(hpdsDirectory + "allObservationsStore.javabin", "rw");
        initialLoad();
        store.saveStore(hpdsDirectory);
        store.dumpStats(hpdsDirectory);
        log.info("ETL process completed.");
    }

    private void initialLoad() throws IOException {
        Crypto.loadDefaultKey();
        Reader in = new FileReader(hpdsDirectory + "allConcepts.csv");
        Iterable<CSVRecord> records = CSVFormat.DEFAULT
                .withFirstRecordAsHeader()
                .withSkipHeaderRecord(true)
                .parse(new BufferedReader(in, 1024 * 1024));

        CSVConfig csvConfig = configLoader.getConfigFor("allConcepts");

        final PhenoCube[] currentConcept = new PhenoCube[1];
        for (CSVRecord record : records) {
            processRecord(currentConcept, record, csvConfig);
        }

    }

    private void processRecord(final PhenoCube[] currentConcept, CSVRecord record, CSVConfig csvConfig) {
        if (record.size() < 4) {
            log.info("Record number {} had less records than we exgpected so we are skipping it.", record.getRecordNumber());
            return;
        }

        String conceptPath = CSVParserUtil.parseConceptPath(record, DO_VARNAME_ROLLUP, csvConfig);
        if (conceptPath == null || conceptPath.isEmpty()) {
            log.info("Record number {} had no concept path so we are skipping it.", record.getRecordNumber());
            return;
        }

        String numericValue = record.get(CSVParserUtil.NUMERIC_VALUE);
        boolean isAlpha = (numericValue == null || numericValue.isEmpty());
        String value = isAlpha ? record.get(CSVParserUtil.TEXT_VALUE) : numericValue;
        currentConcept[0] = getPhenoCube(currentConcept[0], conceptPath, isAlpha);

        if (value != null && !value.trim().isEmpty() &&
                ((isAlpha && currentConcept[0].vType == String.class) || (!isAlpha && currentConcept[0].vType == Double.class))) {
            value = value.trim();
            currentConcept[0].setColumnWidth(isAlpha ? Math.max(currentConcept[0].getColumnWidth(), value.getBytes().length) : Double.BYTES);
            int patientId = Integer.parseInt(record.get(CSVParserUtil.PATIENT_NUM));
            Date date = null;
            if (record.size() > 4 && record.get(CSVParserUtil.DATETIME) != null && !record.get(CSVParserUtil.DATETIME).isEmpty()) {
                date = new Date(Long.parseLong(record.get(CSVParserUtil.DATETIME)));
            }
            currentConcept[0].add(patientId, isAlpha ? value : Double.parseDouble(value), date);
            store.allIds.add(patientId);
        }
    }

    private PhenoCube getPhenoCube(PhenoCube currentConcept, String conceptPath, boolean isAlpha) {
        if (currentConcept == null || !currentConcept.name.equals(conceptPath)) {
            currentConcept = store.store.getIfPresent(conceptPath);
            if (currentConcept == null) {
                // Invalidating
                store.store.invalidateAll(); // force onremoval to free up cache per concept
                store.store.cleanUp();
                currentConcept = new PhenoCube(conceptPath, isAlpha ? String.class : Double.class);
                store.store.put(conceptPath, currentConcept);
            }
        }
        return currentConcept;
    }
}
package edu.harvard.hms.dbmi.avillach.hpds.etl.phenotype.csv;

import edu.harvard.hms.dbmi.avillach.hpds.crypto.Crypto;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.PhenoCube;
import edu.harvard.hms.dbmi.avillach.hpds.etl.LoadingStore;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.Date;

@SpringBootApplication
@ComponentScan(
        basePackages = "edu.harvard.hms.dbmi.avillach.hpds",
        includeFilters = @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, classes = Crypto.class)
)
public class CSVLoaderNewSearch {

    private static final Logger log = LoggerFactory.getLogger(CSVLoaderNewSearch.class);

    public static void main(String[] args) {
        SpringApplication.run(CSVLoaderNewSearch.class, args);
    }

    @Bean
    CommandLineRunner runCSVLoader(CSVLoaderService csvLoaderService) {
        return args -> {
            boolean doRollup = args.length > 0 && args[0].equalsIgnoreCase("NO_ROLLUP");
            csvLoaderService.runEtlProcess(doRollup);
        };
    }
}

@Service
@ConfigurationProperties(prefix = "etl")
class CSVLoaderService {

    private static final Logger log = LoggerFactory.getLogger(CSVLoaderService.class);
    private final LoadingStore store = new LoadingStore();

    private String hpdsDirectory = "./"; // Default directory, can be overridden via application.properties

    public void setHpdsDirectory(String hpdsDirectory) {
        this.hpdsDirectory = hpdsDirectory;
    }

    public void runEtlProcess(boolean doRollup) throws IOException {
        log.info("Starting ETL process... Rollup Enabled: {}", !doRollup);

        store.allObservationsStore = new RandomAccessFile(hpdsDirectory + "allObservationsStore.javabin", "rw");
        initialLoad(doRollup);
        store.saveStore(hpdsDirectory);

        log.info("ETL process completed.");
    }

    private void initialLoad(boolean doRollup) throws IOException {
        Crypto.loadDefaultKey();
        Reader in = new FileReader(hpdsDirectory + "allConcepts.csv");
        Iterable<CSVRecord> records = CSVFormat.DEFAULT.withSkipHeaderRecord().withFirstRecordAsHeader().parse(new BufferedReader(in, 1024 * 1024));

        final PhenoCube[] currentConcept = new PhenoCube[1];
        for (CSVRecord record : records) {
            processRecord(currentConcept, record, doRollup);
        }
    }

    private void processRecord(final PhenoCube[] currentConcept, CSVRecord record, boolean doRollup) {
        if (record.size() < 4) {
            log.warn("Skipping record #{} due to missing fields.", record.getRecordNumber());
            return;
        }

        String conceptPath = CSVParserUtil.parseConceptPath(record, doRollup);
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
                currentConcept = new PhenoCube(conceptPath, isAlpha ? String.class : Double.class);
                store.store.put(conceptPath, currentConcept);
            }
        }
        return currentConcept;
    }
}
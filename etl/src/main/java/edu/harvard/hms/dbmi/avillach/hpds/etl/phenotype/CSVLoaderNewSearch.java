package edu.harvard.hms.dbmi.avillach.hpds.etl.phenotype;

import java.io.*;
import java.util.Date;
import java.util.concurrent.ExecutionException;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheLoader.InvalidCacheLoadException;

import edu.harvard.hms.dbmi.avillach.hpds.crypto.Crypto;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.PhenoCube;

@SuppressWarnings({"unchecked", "rawtypes"})
public class CSVLoaderNewSearch {

	private static LoadingStore store = new LoadingStore();

	private static Logger log = LoggerFactory.getLogger(CSVLoaderNewSearch.class);

	private static final int PATIENT_NUM = 0;

	private static final int CONCEPT_PATH = 1;

	private static final int NUMERIC_VALUE = 2;

	private static final int TEXT_VALUE = 3;

	private static final int DATETIME = 4;

	private static boolean DO_VARNAME_ROLLUP = false;

	public static void main(String[] args) throws IOException {
		if(args.length > 1) {
			if(args[0].equalsIgnoreCase("NO_ROLLUP")) {
				log.info("NO_ROLLUP SET.");
				DO_VARNAME_ROLLUP = false;
			}
		}
		store.allObservationsStore = new RandomAccessFile("/opt/local/hpds/allObservationsStore.javabin", "rw");
		initialLoad();
		store.saveStore();
		store.dumpStatsAndColumnMeta();
	}

	private static void initialLoad() throws IOException {
		Crypto.loadDefaultKey();
		Reader in = new FileReader("/opt/local/hpds/allConcepts.csv");
		Iterable<CSVRecord> records = CSVFormat.DEFAULT.withSkipHeaderRecord().withFirstRecordAsHeader().parse(new BufferedReader(in, 1024*1024));

		final PhenoCube[] currentConcept = new PhenoCube[1];
		for (CSVRecord record : records) {
			processRecord(currentConcept, record);
		}
	}

    private static void processRecord(final PhenoCube[] currentConcept, CSVRecord record) {
        if (record.size() < 4) {
            log.info("Record number {} had less records than we expected so we are skipping it.", record.getRecordNumber());
            return;
        }

        // Gate: PATIENT_NUM must be a valid int
        final String patientStr = record.get(PATIENT_NUM);
        final int patientId;
        if (patientStr == null || patientStr.trim().isEmpty()) {
            log.warn("Skipping record {}: missing PATIENT_NUM. Record={}", record.getRecordNumber(), record);
            return;
        }
        try {
            patientId = Integer.parseInt(patientStr.trim());
        } catch (NumberFormatException nfe) {
            // Log the raw value + a little context to debug upstream data
            String concept = safeGet(record, CONCEPT_PATH);
            String textVal = safeGet(record, TEXT_VALUE);
            String numVal  = safeGet(record, NUMERIC_VALUE);
            log.warn(
                "Skipping record {}: invalid PATIENT_NUM='{}'. conceptPath='{}' text='{}' numeric='{}'. FullRecord={}",
                record.getRecordNumber(), patientStr, concept, textVal, numVal, record
            );
            return;
        }

        try {
            String conceptPathFromRow = record.get(CONCEPT_PATH);
            String[] segments = conceptPathFromRow.split("\\\\");
            for (int x = 0; x < segments.length; x++) {
                segments[x] = segments[x].trim();
            }
            conceptPathFromRow = String.join("\\", segments) + "\\";
            conceptPathFromRow = conceptPathFromRow.replaceAll("\\ufffd", "");

            String textValueFromRow = record.get(TEXT_VALUE) == null ? null : record.get(TEXT_VALUE).trim();
            if (textValueFromRow != null) {
                textValueFromRow = textValueFromRow.replaceAll("\\ufffd", "");
            }

            String conceptPath;
            if (DO_VARNAME_ROLLUP) {
                conceptPath = conceptPathFromRow.endsWith("\\" + textValueFromRow + "\\")
                    ? conceptPathFromRow.replaceAll("\\\\[^\\\\]*\\\\$", "\\\\")
                    : conceptPathFromRow;
            } else {
                conceptPath = conceptPathFromRow;
            }

            String numericValue = record.get(NUMERIC_VALUE);
            boolean isAlpha = (numericValue == null || numericValue.isEmpty());

            if (currentConcept[0] == null || !currentConcept[0].name.equals(conceptPath)) {
                System.out.println(conceptPath);
                try {
                    currentConcept[0] = store.store.get(conceptPath);
                } catch (InvalidCacheLoadException e) {
                    currentConcept[0] = new PhenoCube(conceptPath, isAlpha ? String.class : Double.class);
                    store.store.put(conceptPath, currentConcept[0]);
                }
            }

            String value = isAlpha ? record.get(TEXT_VALUE) : numericValue;

            if (value != null
                && !value.trim().isEmpty()
                && ((isAlpha && currentConcept[0].vType == String.class) || (!isAlpha && currentConcept[0].vType == Double.class))) {

                value = value.trim();
                currentConcept[0].setColumnWidth(
                    isAlpha ? Math.max(currentConcept[0].getColumnWidth(), value.getBytes().length) : Double.BYTES
                );

                Date date = null;
                if (record.size() > 4) {
                    date = parseUtcDatetimeOrNull(record.get(DATETIME), record);
                }

                currentConcept[0].add(patientId, isAlpha ? value : Double.parseDouble(value), date);
                store.allIds.add(patientId);
            }
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    private static String safeGet(CSVRecord record, int idx) {
        try {
            String v = record.get(idx);
            return v == null ? "" : v;
        } catch (Exception e) {
            return "";
        }
    }

    private static Date parseUtcDatetimeOrNull(String raw, CSVRecord record) {
        if (raw == null) return null;
        String s = raw.trim();
        if (s.isEmpty()) return null;

        // 1) Epoch millis / seconds
        try {
            long v = Long.parseLong(s);
            if (v > 0 && v < 10_000_000_000L) v *= 1000L; // seconds -> millis
            return new Date(v);
        } catch (NumberFormatException ignored) {
        }

        // Normalize common variants
        // - allow "yyyy-MM-dd HH:mm:ss" by converting space -> 'T'
        // - allow trailing 'Z' or offsets naturally via ISO parsers
        String isoLike = s.contains("T") ? s : s.replace(' ', 'T');

        // 2) ISO 8601 with Z (Instant) e.g. 2023-11-25T14:00:00Z or 2023-11-25T14:00:00.123Z
        try {
            return Date.from(java.time.Instant.parse(isoLike));
        } catch (Exception ignored) {
        }

        // 3) ISO 8601 with offset e.g. 2023-11-25T14:00:00-05:00 or +00:00
        try {
            return Date.from(java.time.OffsetDateTime.parse(isoLike).toInstant());
        } catch (Exception ignored) {
        }

        // 4) ISO local datetime (no zone) -> assume UTC
        //    e.g. 2023-11-25T14:00:00 or 2023-11-25T14:00:00.123
        java.time.format.DateTimeFormatter[] localFmts = new java.time.format.DateTimeFormatter[] {
            java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME,
            java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"),
            java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")
        };

        for (java.time.format.DateTimeFormatter fmt : localFmts) {
            try {
                java.time.LocalDateTime ldt = java.time.LocalDateTime.parse(isoLike, fmt);
                return Date.from(ldt.toInstant(java.time.ZoneOffset.UTC));
            } catch (Exception ignored) {
            }
        }

        log.warn("Unparseable DATETIME (expected UTC / ISO-8601 / epoch) on record {}: DATETIME='{}' (leaving null)",
            record.getRecordNumber(), raw);
        return null;
    }
}

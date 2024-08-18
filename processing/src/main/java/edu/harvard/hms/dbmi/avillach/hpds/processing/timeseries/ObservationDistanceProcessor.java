package edu.harvard.hms.dbmi.avillach.hpds.processing.timeseries;

import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.ColumnMeta;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.KeyAndValue;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.PhenoCube;
import edu.harvard.hms.dbmi.avillach.hpds.data.query.Query;
import edu.harvard.hms.dbmi.avillach.hpds.processing.AbstractProcessor;
import edu.harvard.hms.dbmi.avillach.hpds.processing.AsyncResult;
import edu.harvard.hms.dbmi.avillach.hpds.processing.HpdsProcessor;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Component
public class ObservationDistanceProcessor implements HpdsProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(ObservationDistanceProcessor.class);

    private final AbstractProcessor abstractProcessor;

    @Autowired
    public ObservationDistanceProcessor(AbstractProcessor abstractProcessor) {
        this.abstractProcessor = abstractProcessor;
    }

    @Override
    public String[] getHeaderRow(Query query) {
        return new String[]{"QUERIED_CONCEPT", "PATIENT_ID", "FIRST_OBSERVATION", "LAST_OBSERVATION"};
    }

    private static final class PatientJourney {
        final long firstObs, lastObs;
        final String path;
        final int patient;

        public PatientJourney(String path, int patient) {
            this(Long.MAX_VALUE, Long.MIN_VALUE, path, patient);
        }

        public PatientJourney(long firstObs, long lastObs, String path, int patient) {
            this.firstObs = firstObs;
            this.lastObs = lastObs;
            this.path = path;
            this.patient = patient;
        }

        public PatientJourney from(long newFirst, long newLast) {
            newFirst = Long.min(newFirst, firstObs);
            newLast = Long.max(newLast, lastObs);
            if (newFirst == firstObs && newLast == lastObs) {
                return this;
            }
            return new PatientJourney(newFirst, newLast, path, patient);
        }

        public long distance() {
            return lastObs - firstObs;
        }
    }

    @Override
    public void runQuery(Query query, AsyncResult asyncResult) {

    }

    public void writeJourney(Query query) {
        try {
            String tmpPath = File.createTempFile(System.nanoTime() + "journey", ".tsv").getAbsolutePath();
            Set<Integer> recordedJourneys = new HashSet<>();
            for (String concept : query.getAnyRecordOf()) {
                LOG.info("Creating user journeys for concept {}", concept);
                Optional<PhenoCube<?>> maybeCube = abstractProcessor.nullableGetCube(concept);
                if (maybeCube.isEmpty()) {
                    continue;
                }
                maybeCube.get().keyBasedIndex().stream()
                    .filter(Predicate.not(recordedJourneys::contains))
                    .map(id -> new PatientJourney(concept, id))
                    .map(this::updateJourney)
                    .sorted((a, b) -> -Long.compare(b.distance(), a.distance()))
                    .peek(j -> recordedJourneys.add(j.patient))
                    .limit(100)
                    .forEach(journey -> write(Path.of(tmpPath), journey));
                LOG.info("Done creating user journeys for concept {} to {}", concept, tmpPath);
            }
        } catch (IOException e) {
            LOG.error("Can't create temp file", e);
            return;
        }
    }

    private void write(Path path, PatientJourney journey) {
        try {
            List<String> row = List.of(journey.path, journey.patient + "", journey.distance() + "");
            Files.write(path, (String.join("\t", row) + "\n").getBytes());
        } catch (IOException e) {
            LOG.error("Failed to write", e);
        }
    }

    private PatientJourney updateJourney(PatientJourney journey) {
        LOG.info("Creating user journey for path {}", journey.path);
        for (ColumnMeta value : abstractProcessor.getDictionary().values()) {
            PatientJourney finalJourney = journey;
            journey = abstractProcessor.nullableGetCube(value.getName())
                .map(cube -> updateJourneyWithCube(finalJourney, cube))
                .orElse(journey);
        }
        LOG.info("Done creating journey for path {}", journey.path);
        return journey;
    }

    /**
     * Update journey with new min / max timestamps from cube for this patient.
     * If min / max values are unchanged, original journey object is returned.
     */
    private PatientJourney updateJourneyWithCube(PatientJourney journey, PhenoCube<?> cube) {
        List<? extends KeyAndValue<?>> values = cube.getValuesForKeys(Set.of(journey.patient));
        if (values == null) {
            return journey;
        }
        long last = values.stream()
            .mapToLong(KeyAndValue::getTimestamp)
            .max()
            .orElse(journey.lastObs);
        long first = values.stream()
            .mapToLong(KeyAndValue::getTimestamp)
            .min()
            .orElse(journey.firstObs);

        return journey.from(first, last);
    }
}

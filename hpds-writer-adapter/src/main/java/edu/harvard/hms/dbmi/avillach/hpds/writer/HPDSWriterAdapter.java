package edu.harvard.hms.dbmi.avillach.hpds.writer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Date;
import java.util.List;

/**
 * Facade for writing observations to HPDS stores.
 *
 * This adapter isolates the HPDS dependency surface area and provides a clean interface
 * for ingestion pipelines.
 *
 * Thread-safety: This class is thread-safe for acceptBatch() calls, but closeAndSave()
 * must be called exactly once after all batches are submitted.
 */
public class HPDSWriterAdapter implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(HPDSWriterAdapter.class);

    private final SpoolingLoadingStore store;
    private volatile boolean closed = false;

    public HPDSWriterAdapter(Path spoolDirectory, String outputDirectory, String encryptionKeyName, int cacheSize, int maxObservationsPerConcept) {
        this.store = new SpoolingLoadingStore(spoolDirectory, outputDirectory, encryptionKeyName, cacheSize, maxObservationsPerConcept);
        log.info("Initialized HPDS writer adapter: spool={}, output={}, cacheSize={}, maxObsPerConcept={}",
                spoolDirectory, outputDirectory, cacheSize, maxObservationsPerConcept);
    }

    /**
     * Default constructor with standard paths.
     */
    public HPDSWriterAdapter() {
        this(
            Path.of("/opt/local/hpds/spool"),
            "/opt/local/hpds/",
            "default",
            16,
            5_000_000  // Default: 5M observations per concept
        );
    }

    /**
     * Accepts a batch of observations for writing.
     *
     * Observations are validated and added to the appropriate concept cubes.
     * Invalid observations are logged and skipped.
     *
     * This method is thread-safe and can be called concurrently from multiple producers.
     *
     * @param batch list of observation rows to ingest
     * @return number of observations successfully accepted
     */
    public int acceptBatch(List<ObservationRow> batch) {
        if (closed) {
            throw new IllegalStateException("Writer adapter is closed");
        }

        int accepted = 0;
        for (ObservationRow row : batch) {
            if (!row.isValid()) {
                log.warn("Invalid observation (must have exactly one value): patientNum={}, conceptPath={}",
                    row.patientNum(), row.conceptPath());
                continue;
            }

            try {
                Date timestamp = row.dateTime() != null ? Date.from(row.dateTime()) : null;
                Comparable<?> value = row.getValue();

                store.addObservation(row.patientNum(), row.conceptPath(), value, timestamp);
                accepted++;

            } catch (Exception e) {
                log.error("Failed to add observation: patientNum={}, conceptPath={}",
                    row.patientNum(), row.conceptPath(), e);
            }
        }

        if (accepted < batch.size()) {
            log.warn("Batch partially accepted: {}/{} observations", accepted, batch.size());
        }

        return accepted;
    }

    /**
     * Closes the adapter and finalizes all HPDS stores.
     *
     * This method:
     * 1. Flushes all in-memory caches
     * 2. Merges all spooled partials per concept
     * 3. Writes each concept exactly once to allObservationsStore
     * 4. Writes columnMeta.javabin
     * 5. Cleans up spool files
     *
     * This method must be called exactly once and is NOT thread-safe.
     *
     * @throws IOException if finalization fails
     */
    public synchronized void closeAndSave() throws IOException {
        if (closed) {
            log.warn("Writer adapter already closed");
            return;
        }

        log.info("Closing and finalizing HPDS writer adapter");
        try {
            store.saveStore();
            closed = true;
            log.info("HPDS writer adapter closed successfully");
        } catch (IOException e) {
            log.error("Failed to finalize HPDS stores", e);
            throw e;
        }
    }

    @Override
    public void close() throws IOException {
        closeAndSave();
    }

    /**
     * Returns the total number of unique patient IDs encountered.
     */
    public int getPatientCount() {
        return store.getAllIds().size();
    }
}

package edu.harvard.hms.dbmi.avillach.hpds.ingest.failure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * JSONL failure sink for Splunk/Elastic consumption.
 *
 * Thread-safe. Writes one JSON object per line to a file.
 */
public class FailureSink implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(FailureSink.class);

    private final Path outputFile;
    private final BufferedWriter writer;
    private final ObjectMapper mapper;

    // Rollup counters
    private final ConcurrentHashMap<String, ConcurrentHashMap<FailureReason, AtomicLong>> rollupCounters = new ConcurrentHashMap<>();
    private final AtomicLong totalFailures = new AtomicLong(0);

    public FailureSink(Path outputFile) throws IOException {
        this.outputFile = outputFile;
        this.writer = Files.newBufferedWriter(outputFile,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.WRITE);

        this.mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        log.info("Initialized failure sink: {}", outputFile);
    }

    /**
     * Records a failure (thread-safe).
     */
    public synchronized void recordFailure(FailureRecord record) {
        try {
            String json = mapper.writeValueAsString(record);
            writer.write(json);
            writer.newLine();

            // Update rollup counters
            rollupCounters
                .computeIfAbsent(record.dataset(), k -> new ConcurrentHashMap<>())
                .computeIfAbsent(record.reasonCode(), k -> new AtomicLong(0))
                .incrementAndGet();

            totalFailures.incrementAndGet();

        } catch (IOException e) {
            log.error("Failed to write failure record", e);
        }
    }

    /**
     * Writes rollup summary at end of run.
     */
    public synchronized void writeRollup() throws IOException {
        writer.write("\n--- ROLLUP SUMMARY ---\n");

        for (Map.Entry<String, ConcurrentHashMap<FailureReason, AtomicLong>> datasetEntry : rollupCounters.entrySet()) {
            String dataset = datasetEntry.getKey();
            writer.write(String.format("Dataset: %s\n", dataset));

            for (Map.Entry<FailureReason, AtomicLong> reasonEntry : datasetEntry.getValue().entrySet()) {
                writer.write(String.format("  %s: %d\n", reasonEntry.getKey(), reasonEntry.getValue().get()));
            }
        }

        writer.write(String.format("Total Failures: %d\n", totalFailures.get()));
        writer.flush();

        log.info("Wrote rollup summary: {} total failures", totalFailures.get());
    }

    public long getTotalFailures() {
        return totalFailures.get();
    }

    @Override
    public void close() throws IOException {
        writeRollup();
        writer.close();
        log.info("Closed failure sink: {}", outputFile);
    }
}

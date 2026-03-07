package edu.harvard.hms.dbmi.avillach.hpds.ingest.producer;

import edu.harvard.hms.dbmi.avillach.hpds.ingest.failure.FailureSink;
import edu.harvard.hms.dbmi.avillach.hpds.writer.ObservationRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Consumer;

/**
 * Processes large CSV files in parallel by chunking them into byte-range segments.
 *
 * <p>Strategy:
 * <ol>
 *   <li>Pre-scan file to find byte offsets for chunk boundaries (aligned to row boundaries)</li>
 *   <li>Split file into N chunks based on estimated rows per chunk</li>
 *   <li>Process each chunk in parallel using virtual threads</li>
 *   <li>Each chunk reads its portion independently via RandomAccessFile</li>
 * </ol>
 *
 */
public class CsvChunkProcessor {
    private static final Logger log = LoggerFactory.getLogger(CsvChunkProcessor.class);

    // Default tuning parameters (can be made configurable later)
    private static final long CHUNK_SIZE_ROWS = 1_000_000L; // 1M rows per chunk
    private static final int ESTIMATION_SAMPLE_SIZE = 10_000; // Sample first 10K rows for size estimation

    private final String runId;
    private final FailureSink failureSink;

    /**
     * Chunk boundary record: byte offsets for a segment of the file.
     */
    private record ChunkBoundary(long startOffset, long endOffset, int chunkIndex) {}

    /**
     * Result of processing a single chunk.
     */
    private record ChunkResult(int chunkIndex, long rowsProcessed, Exception error) {}

    public CsvChunkProcessor(String runId, FailureSink failureSink) {
        this.runId = runId;
        this.failureSink = failureSink;
    }

    /**
     * Processes a large CSV file in parallel by splitting into chunks.
     *
     * @param filePath path to large CSV file
     * @param consumer callback for each batch of observations
     * @param batchSize number of rows per batch
     * @param producer CSV observation producer for sequential fallback
     * @throws IOException if file processing fails
     */
    public void processLargeFileInParallel(Path filePath,
                                           Consumer<List<ObservationRow>> consumer,
                                           int batchSize,
                                           CsvObservationProducer producer) throws IOException {
        log.info("Processing large CSV file in parallel: {}", filePath);

        // Step 1: Build chunk boundaries (pre-scan pass)
        List<ChunkBoundary> chunks = buildChunkBoundaries(filePath, CHUNK_SIZE_ROWS);

        if (chunks.size() == 1) {
            // File is too small or couldn't be chunked, fall back to sequential
            log.info("File has only 1 chunk, using sequential processing: {}", filePath);
            producer.processFile(filePath, consumer, batchSize);
            return;
        }

        // Step 2: Process chunks in parallel with virtual threads
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        List<Future<ChunkResult>> futures = new ArrayList<>();

        try {
            // Submit all chunks
            for (ChunkBoundary chunk : chunks) {
                Future<ChunkResult> future = executor.submit(() ->
                    processChunk(filePath, chunk, consumer, batchSize, producer)
                );
                futures.add(future);
            }

            // Step 3: Collect results
            long totalRowsProcessed = 0;
            int failedChunks = 0;

            for (Future<ChunkResult> future : futures) {
                try {
                    ChunkResult result = future.get();
                    totalRowsProcessed += result.rowsProcessed();

                    if (result.error() != null) {
                        failedChunks++;
                        log.error("Chunk {} failed: {}", result.chunkIndex(), result.error().getMessage());
                    }
                } catch (InterruptedException | ExecutionException e) {
                    failedChunks++;
                    log.error("Failed to retrieve chunk result", e);
                }
            }

            log.info("Completed parallel CSV processing: {} rows from {} chunks ({} failed)",
                     totalRowsProcessed, chunks.size(), failedChunks);

        } finally {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(1, TimeUnit.HOURS)) {
                    log.warn("CSV chunk processor did not terminate within 1 hour");
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                log.error("Interrupted while waiting for CSV chunk processor to terminate", e);
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Builds chunk boundaries by pre-scanning the file to estimate byte offsets.
     *
     * <p>Algorithm:
     * <ol>
     *   <li>Sample first N rows to estimate bytes per row</li>
     *   <li>Calculate target chunk size in bytes</li>
     *   <li>Walk file, seeking to chunk end estimates</li>
     *   <li>Align each boundary to next newline (complete row)</li>
     * </ol>
     */
    private List<ChunkBoundary> buildChunkBoundaries(Path filePath, long chunkSizeRows) throws IOException {
        List<ChunkBoundary> chunks = new ArrayList<>();
        long fileSize = Files.size(filePath);

        // Step 1: Estimate bytes per row from first N rows
        long estimatedBytesPerRow;
        try (BufferedReader reader = Files.newBufferedReader(filePath)) {
            long bytesRead = 0;
            int rowsRead = 0;
            String line;

            while ((line = reader.readLine()) != null && rowsRead < ESTIMATION_SAMPLE_SIZE) {
                bytesRead += line.length() + 1; // +1 for newline
                rowsRead++;
            }

            estimatedBytesPerRow = rowsRead > 0 ? bytesRead / rowsRead : 1000; // Fallback: 1KB/row
            log.debug("Estimated bytes per row: {} (sampled {} rows)", estimatedBytesPerRow, rowsRead);
        }

        // Step 2: Calculate chunk boundaries
        long targetChunkBytes = chunkSizeRows * estimatedBytesPerRow;
        long currentOffset = 0;
        int chunkIndex = 0;

        try (RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "r")) {
            // Skip header if present
            String firstLine = raf.readLine();
            if (firstLine != null && detectHeader(firstLine)) {
                currentOffset = raf.getFilePointer();
                log.info("Detected header, skipping first line (starting at offset: {})", currentOffset);
            } else {
                raf.seek(0); // No header, reset to start
                currentOffset = 0;
            }

            while (currentOffset < fileSize) {
                long chunkEndTarget = Math.min(currentOffset + targetChunkBytes, fileSize);

                // Find next newline after target offset (align to row boundary)
                raf.seek(chunkEndTarget);
                if (chunkEndTarget < fileSize) {
                    raf.readLine(); // Advance to end of current line
                }
                long alignedEndOffset = Math.min(raf.getFilePointer(), fileSize);

                chunks.add(new ChunkBoundary(currentOffset, alignedEndOffset, chunkIndex++));
                currentOffset = alignedEndOffset;
            }
        }

        log.info("Split file into {} chunks (estimated {} rows per chunk, {} MB per chunk)",
                 chunks.size(), chunkSizeRows, targetChunkBytes / 1024 / 1024);
        return chunks;
    }

    /**
     * Processes a single chunk using bounded file reading.
     */
    private ChunkResult processChunk(Path filePath, ChunkBoundary chunk,
                                     Consumer<List<ObservationRow>> consumer,
                                     int batchSize,
                                     CsvObservationProducer producer) {
        try {
            log.debug("Processing chunk {} (offset {}-{}, {} MB)",
                      chunk.chunkIndex(), chunk.startOffset(), chunk.endOffset(),
                      (chunk.endOffset() - chunk.startOffset()) / 1024 / 1024);

            // Open file at chunk start offset and read until end offset
            try (RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "r")) {
                raf.seek(chunk.startOffset());

                // Wrap in bounded input stream to limit reading to chunk range
                InputStream boundedInput = new BoundedInputStream(
                    new RandomAccessFileInputStream(raf),
                    chunk.endOffset() - chunk.startOffset()
                );

                // Use producer's parsing logic for this chunk
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(boundedInput))) {
                    long rowsProcessed = producer.processStream(reader, filePath, consumer, batchSize, false);

                    log.debug("Chunk {} complete: {} rows processed", chunk.chunkIndex(), rowsProcessed);
                    return new ChunkResult(chunk.chunkIndex(), rowsProcessed, null);
                }
            }

        } catch (Exception e) {
            log.error("Chunk {} processing failed", chunk.chunkIndex(), e);
            return new ChunkResult(chunk.chunkIndex(), 0, e);
        }
    }

    /**
     * Detects if a line is a valid header row.
     * (Simplified detection - checks for expected column names)
     */
    private boolean detectHeader(String line) {
        if (line == null || line.isBlank()) {
            return false;
        }
        String upper = line.toUpperCase();
        return upper.contains("PATIENT_NUM") &&
               upper.contains("CONCEPT_PATH") &&
               upper.contains("NVAL_NUM") &&
               upper.contains("TVAL_CHAR");
    }

    /**
     * Input stream that reads from RandomAccessFile.
     */
    private static class RandomAccessFileInputStream extends InputStream {
        private final RandomAccessFile raf;

        public RandomAccessFileInputStream(RandomAccessFile raf) {
            this.raf = raf;
        }

        @Override
        public int read() throws IOException {
            return raf.read();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return raf.read(b, off, len);
        }
    }

    /**
     * Input stream wrapper that limits reading to maxBytes.
     * (Similar to Commons IO BoundedInputStream, but self-contained)
     */
    private static class BoundedInputStream extends InputStream {
        private final InputStream delegate;
        private long remaining;

        public BoundedInputStream(InputStream delegate, long maxBytes) {
            this.delegate = delegate;
            this.remaining = maxBytes;
        }

        @Override
        public int read() throws IOException {
            if (remaining <= 0) return -1;
            int b = delegate.read();
            if (b != -1) remaining--;
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (remaining <= 0) return -1;
            int toRead = (int) Math.min(len, remaining);
            int bytesRead = delegate.read(b, off, toRead);
            if (bytesRead > 0) remaining -= bytesRead;
            return bytesRead;
        }
    }
}

package edu.harvard.hms.dbmi.avillach.hpds.processing.flatvcf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class FlatVCFWriter {

    private static final Logger LOG = LoggerFactory.getLogger(FlatVCFWriter.class);

    private final AtomicInteger linesSinceFlush = new AtomicInteger(0);
    private final AtomicInteger totalLines = new AtomicInteger(0);

    // Using this instead of synchronized to make this j21 compatible
    private final ReentrantLock lock = new ReentrantLock(true);

    private final int MAX_ROWS = 10;

    private final FileWriter writer;

    public FlatVCFWriter(FileWriter writer) {
        this.writer = writer;
    }

    protected void writeRow(String row) {
        lock.lock();
        try {
            writer.write(row + "\n");
            if (linesSinceFlush.incrementAndGet() > MAX_ROWS) {
                LOG.info("Wrote {} rows", totalLines.incrementAndGet());
                writer.flush();
            };
        } catch (IOException e) {
            LOG.warn("Error writing", e);
        } finally {
            lock.unlock();
        }
    }

    protected boolean complete() {
        lock.lock();
        try {
            writer.flush();
            writer.close();
        } catch (IOException e) {
            LOG.warn("Error closing", e);
            return false;
        } finally {
            lock.unlock();
        }
        return true;
    }
}

package edu.harvard.hms.dbmi.avillach.hpds.processing.io;

import java.io.IOException;
import java.util.Collection;

public interface ResultWriter {
    void writeHeader(String[] data);

    void writeEntity(Collection<String[]> data);

    void close() throws IOException;
}

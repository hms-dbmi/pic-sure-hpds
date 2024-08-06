package edu.harvard.hms.dbmi.avillach.hpds.processing.io;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

public interface ResultWriter {
    void writeHeader(String[] data);

    void writeEntity(Collection<String[]> data);
    void writeMultiValueEntity(Collection<List<List<String>>> data);

    File getFile();

    void close();
}

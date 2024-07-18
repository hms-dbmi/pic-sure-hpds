package edu.harvard.hms.dbmi.avillach.hpds.processing.io;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class CsvWriter implements ResultWriter {

    private final de.siegmar.fastcsv.writer.CsvWriter csvWriter;

    private final FileWriter fileWriter;

    public CsvWriter(File file) {
        csvWriter = new de.siegmar.fastcsv.writer.CsvWriter();
        try {
            this.fileWriter = new FileWriter(file);
        } catch (IOException e) {
            throw new RuntimeException("IOException while appending temp file : " + file.getAbsolutePath(), e);
        }
    }

    @Override
    public void writeHeader(String[] data) {
        try {
            List<String[]> dataList = new ArrayList<>();
            dataList.add(data);
            csvWriter.write(fileWriter, dataList);
        } catch (IOException e) {
            throw new RuntimeException("IOException while appending to CSV file", e);
        }
    }
    @Override
    public void writeEntity(Collection<String[]> data) {
        try {
            csvWriter.write(fileWriter, data);
        } catch (IOException e) {
            throw new RuntimeException("IOException while appending to CSV file", e);
        }
    }

    @Override
    public void close() throws IOException {
        fileWriter.close();
    }
}

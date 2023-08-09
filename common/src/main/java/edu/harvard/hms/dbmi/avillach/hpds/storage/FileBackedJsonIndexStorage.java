package edu.harvard.hms.dbmi.avillach.hpds.storage;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public abstract class FileBackedJsonIndexStorage <K, V extends Serializable> extends FileBackedByteIndexedStorage<K, V> {
    private static final long serialVersionUID = -1086729119489479152L;

    protected transient ObjectMapper objectMapper = new ObjectMapper();

    public FileBackedJsonIndexStorage(File storageFile) throws FileNotFoundException {
        super(null, null, storageFile);
    }

    protected ByteArrayOutputStream writeObject(V value) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        objectMapper.writeValue(new GZIPOutputStream(out), value);
        return out;
    }

    protected V readObject(byte[] buffer) {
        try {
            return objectMapper.readValue(new GZIPInputStream(new ByteArrayInputStream(buffer)), getTypeReference());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public abstract TypeReference<V> getTypeReference();
}

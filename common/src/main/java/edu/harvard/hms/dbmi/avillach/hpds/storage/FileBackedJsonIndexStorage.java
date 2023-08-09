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

    public void put(K key, V value) throws IOException {
        if(completed) {
            throw new RuntimeException("A completed FileBackedByteIndexedStorage cannot be modified.");
        }
        Long[] recordIndex = store(value);
        index.put(key, recordIndex);
    }

    private Long[] store(V value) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        objectMapper.writeValue(new GZIPOutputStream(out), value);

        Long[] recordIndex = new Long[2];
        synchronized(storage) {
            storage.seek(storage.length());
            recordIndex[0] = storage.getFilePointer();
            storage.write(out.toByteArray());
            recordIndex[1] = storage.getFilePointer() - recordIndex[0];
//			maxStorageSize = storage.getFilePointer();
        }
        return recordIndex;
    }

    public V get(K key) {
        try {
            if(this.storage==null) {
                synchronized(this) {
                    this.open();
                }
            }
            Long[] offsetsInStorage = index.get(key);
            if(offsetsInStorage != null) {
                Long offsetInStorage = index.get(key)[0];
                int offsetLength = index.get(key)[1].intValue();
                if(offsetInStorage != null && offsetLength>0) {
                    byte[] buffer = new byte[offsetLength];
                    synchronized(storage) {
                        storage.seek(offsetInStorage);
                        storage.readFully(buffer);
                    }
                    V readObject = readObject(buffer);
                    return readObject;
                }else {
                    return null;
                }
            } else {
                return null;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected V readObject(byte[] buffer) {
        try {
            return objectMapper.readValue(new GZIPInputStream(new ByteArrayInputStream(buffer)), getTypeReference());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public abstract TypeReference<V> getTypeReference();

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        objectMapper = new ObjectMapper();
    }
}

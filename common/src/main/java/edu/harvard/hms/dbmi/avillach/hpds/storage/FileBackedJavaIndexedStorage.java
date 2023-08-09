package edu.harvard.hms.dbmi.avillach.hpds.storage;

import org.apache.commons.io.output.ByteArrayOutputStream;

import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class FileBackedJavaIndexedStorage <K, V extends Serializable> extends FileBackedByteIndexedStorage<K, V> {
    public FileBackedJavaIndexedStorage(Class<K> keyClass, Class<V> valueClass, File storageFile) throws FileNotFoundException {
        super(keyClass, valueClass, storageFile);
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
        ObjectOutputStream oos = new ObjectOutputStream(new GZIPOutputStream(out));
        oos.writeObject(value);
        oos.flush();
        oos.close();

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
                    ObjectInputStream in = new ObjectInputStream(new GZIPInputStream(new ByteArrayInputStream(buffer)));

                    try {
                        V readObject = (V) in.readObject();
                        return readObject;
                    } finally {
                        in.close();
                    }
                }
            }
            return null;
        } catch (IOException e) {
            throw new UncheckedIOException(e)
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}

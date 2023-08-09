package edu.harvard.hms.dbmi.avillach.hpds.storage;

import java.io.*;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public abstract class FileBackedByteIndexedStorage <K, V extends Serializable> implements Serializable {
	private static final long serialVersionUID = -7297090745384302635L;
	protected transient RandomAccessFile storage;
	protected ConcurrentHashMap<K, Long[]> index;
	protected File storageFile;
	protected boolean completed = false;
	protected Long maxStorageSize;  //leave this in to not break serialization


	public FileBackedByteIndexedStorage(Class<K> keyClass, Class<V> valueClass, File storageFile) throws FileNotFoundException {
		this.index = new ConcurrentHashMap<K, Long[]>();
		this.storageFile = storageFile;
		this.storage = new RandomAccessFile(this.storageFile, "rw");
	}

	public void updateStorageDirectory(File storageDirectory) {
		if (!storageDirectory.isDirectory()) {
			throw new IllegalArgumentException("storageDirectory is not a directory");
		}
		String currentStoreageFilename = storageFile.getName();
		storageFile = new File(storageDirectory, currentStoreageFilename);
	}

	public Set<K> keys(){
		return index.keySet();
	}

	public abstract void put(K key, V value) throws IOException;

	public void load(Iterable<V> values, Function<V, K> mapper) throws IOException {
		//make sure we start fresh
		if(this.storageFile.exists()) {
			this.storageFile.delete();
		}
		this.storage = new RandomAccessFile(storageFile, "rw");
		for(V value : values) {
			put(mapper.apply(value), value);
		}
		this.storage.close();
		complete();
	}

	public void open() throws FileNotFoundException {
		this.storage = new RandomAccessFile(this.storageFile, "rwd");
	}

	public void complete() {
		this.completed = true;
	}

	public boolean isComplete() {
		return this.completed;
	}
	public abstract V get(K key);

	public V getOrELse(K key, V defaultValue) throws IOException {
		V result = get(key);
		return result == null ? defaultValue : result;
	}

}

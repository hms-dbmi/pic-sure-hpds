package edu.harvard.hms.dbmi.avillach.hpds.data.genotype;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import edu.harvard.hms.dbmi.avillach.hpds.storage.FileBackedByteIndexedStorage;

/**
 * This class will create a set of FBBIS objects that allow lookups of variant-spec -> metadata, instead of the 
 * metadata -> variant-spec map that is used for searching and identifying patients. 
 */
public class VariantMetadataIndex implements Serializable {
	public static String VARIANT_METADATA_BIN_FILE = "/opt/local/hpds/all/VariantMetadata.javabin";
	
	private static final long serialVersionUID = 5917054606643971537L;
	private static Logger log = Logger.getLogger(VariantMetadataIndex.class); 
	
	private Map<String,  FileBackedByteIndexedStorage<String, String[]> > indexMap = new HashMap<String,  FileBackedByteIndexedStorage<String, String[]> >();
	private static String fileStoragePrefix = "/opt/local/hpds/all/VariantMetadataStorage";

	/**
	 * This constructor should only be used for testing; we expect the files to be in the default locations in production
	 * @param storageFile
	 * @throws IOException
	 */
	public VariantMetadataIndex(String storageFile) throws IOException { 
		fileStoragePrefix = storageFile;  
	}  
	
	/**
	 * creates a default metadata index that maps variant spec -> metadata using an array of one file per contig.
	 * @throws IOException
	 */
	public VariantMetadataIndex() throws IOException {  
	}

	public void initializeRead() throws FileNotFoundException, IOException, ClassNotFoundException {
		for(String contig : indexMap.keySet()) {
			FileBackedByteIndexedStorage<String, String[]> fbbis = indexMap.get(contig);
			fbbis.open();
		}
		log.info("Initialized metadata index with " + indexMap.size() + " contig stores");
	}

	public String[] findBySingleVariantSpec(String variantSpec) {
		try {
			String contig = variantSpec.substring(0, variantSpec.indexOf(','));
			FileBackedByteIndexedStorage<String, String[]> fbbis = indexMap.get(contig);
			String[] value = (fbbis != null ? fbbis.get(variantSpec) : null);
			return value != null  ? value :  new String[0];
		} catch (IOException e) {
			log.warn("IOException caught looking up variantSpec : " + variantSpec, e);
			return new String[0];
		}
	}

	public Map<String, String[]> findByMultipleVariantSpec(Collection<String> varientSpecList) {
		return varientSpecList.stream().collect(Collectors.toMap(
				variant->{return variant;},
				variant->{return findBySingleVariantSpec(variant);}
				));
	}

	public void put(String variantSpec, String[] array) throws IOException {
		String contig = variantSpec.substring(0, variantSpec.indexOf(','));
		FileBackedByteIndexedStorage<String, String[]> fbbis = indexMap.get(contig);
		
		if(fbbis == null) {
			String filePath = fileStoragePrefix + "_" + contig + ".bin";
			fbbis = new FileBackedByteIndexedStorage<>(String.class, String[].class, new File(filePath));
			indexMap.put(contig, fbbis);
		}
		
		fbbis.put(variantSpec, array);
	}

	public void complete() {
		for(String contig : indexMap.keySet()) {
			FileBackedByteIndexedStorage<String, String[]> fbbis = indexMap.get(contig);
			fbbis.complete();
		}
	}
}
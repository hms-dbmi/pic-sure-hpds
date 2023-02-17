package edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.util;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.ColumnMeta;
public class UpdateClinicalVariableCounts {
    protected static final String COLUMN_META_FILE = "/opt/local/hpds/columnMeta.javabin";
    protected static Set<Integer> allIds;

    public static void main(String[] args) throws ClassNotFoundException, FileNotFoundException, IOException {
	
		if(!Files.exists(Paths.get(COLUMN_META_FILE))) {
			throw new RuntimeException("Column Metadata file - " + COLUMN_META_FILE  + " -  does not exist!");
		}			

		TreeMap<String, ColumnMeta> metadata = updateMetadata();
			
		ObjectOutputStream metaOut = new ObjectOutputStream(new GZIPOutputStream(new FileOutputStream(new File(COLUMN_META_FILE))));
			
		metaOut.writeObject(metadata); 
		
		metaOut.writeObject(allIds); 
		metaOut.flush();
		metaOut.close();
	}

    protected static TreeMap<String, ColumnMeta> updateMetadata(){
        try (ObjectInputStream objectInputStream = new ObjectInputStream(new GZIPInputStream(new FileInputStream(COLUMN_META_FILE)))){
		
			TreeMap<String, ColumnMeta> metastore = (TreeMap<String, ColumnMeta>) objectInputStream.readObject();
			
			allIds = (TreeSet<Integer>) objectInputStream.readObject();
			
			System.out.println("allIds size = " + allIds.size());			
			
			return metastore;
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
			throw new RuntimeException("Could not load metastore");
		} 


    }
}

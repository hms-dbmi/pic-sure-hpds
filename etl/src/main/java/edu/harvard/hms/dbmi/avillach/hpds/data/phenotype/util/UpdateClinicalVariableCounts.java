package edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.util;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.ColumnMeta;
public class UpdateClinicalVariableCounts {
    protected static final String COLUMN_META_FILE = "/opt/local/hpds/columnMeta.javabin";

    public static void main(String[] args) throws ClassNotFoundException, FileNotFoundException, IOException {
	
		if(!Files.exists(Paths.get(COLUMN_META_FILE))) {
			throw new RuntimeException("Column Metadata file - " + COLUMN_META_FILE  + " -  does not exist!");
		}			

		TreeMap<String, Integer> counts = updateCounts();
		Set<Map.Entry<String, Integer>> output = counts.entrySet();
        output.forEach( entry -> {
            System.out.println(entry.getKey() + "->" + entry.getValue());
        });	
		
	}

    protected static TreeMap<String, Integer> updateCounts(){
        TreeMap<String, Integer> counts = new TreeMap<String, Integer>();
        try (ObjectInputStream objectInputStream = new ObjectInputStream(new GZIPInputStream(new FileInputStream(COLUMN_META_FILE)))){
		
			TreeMap<String, ColumnMeta> metastore = (TreeMap<String, ColumnMeta>) objectInputStream.readObject();
			Collection<ColumnMeta> columnMetas = metastore.values();
            columnMetas.forEach(value -> {
                String backslashRegex = "\\";
                String studyId = value.getName().split(Pattern.quote(backslashRegex))[1];
                
                if(counts.containsKey(studyId)){
                    counts.replace(studyId, counts.get(studyId)+1);
                    System.out.println(studyId + " updated");
                }
                else{
                    System.out.println(studyId + " added");
                    counts.put(studyId, 1);     
                }
            });
			
						
			
			
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
			throw new RuntimeException("Could not load metastore");
		} 
        return counts;

    }
}

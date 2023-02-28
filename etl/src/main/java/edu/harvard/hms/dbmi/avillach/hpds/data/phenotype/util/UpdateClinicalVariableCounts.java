package edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
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
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class UpdateClinicalVariableCounts {

    // protected static final String COLUMN_META_FILE =
    // "/opt/local/hpds/columnMeta.javabin";
    // protected static final String META_JSON_FILE =
    // "/opt/local/hpds/metadata.json";
    protected static final String COLUMN_META_FILE = "ToDelete/columnMeta.javabin";
    protected static final String META_JSON_FILE = "ToDelete/metadata.json";

    public static void main(String[] args)
            throws ClassNotFoundException, FileNotFoundException, IOException, ParseException {

        if (!Files.exists(Paths.get(COLUMN_META_FILE))) {
            throw new RuntimeException("Column Metadata file - " + COLUMN_META_FILE + " -  does not exist!");
        }
        if (!Files.exists(Paths.get(META_JSON_FILE))) {
            throw new RuntimeException("Metadata JSON file - " + META_JSON_FILE + " -  does not exist!");
        }

        TreeMap<String, Integer> counts = getCounts();
        Set<Map.Entry<String, Integer>> output = counts.entrySet();
        output.forEach(entry -> {
            System.out.println(entry.getKey() + "->" + entry.getValue());
        });
        JSONArray updatedJson = updateCountsInJson(META_JSON_FILE, counts);
        FileWriter outputFile = new FileWriter(META_JSON_FILE);
        outputFile.write(updatedJson.toJSONString());
        outputFile.flush();
        outputFile.close();

    }

    protected static TreeMap<String, Integer> getCounts() {
        TreeMap<String, Integer> counts = new TreeMap<String, Integer>();
        try (ObjectInputStream objectInputStream = new ObjectInputStream(
                new GZIPInputStream(new FileInputStream(COLUMN_META_FILE)))) {

            TreeMap<String, ColumnMeta> metastore = (TreeMap<String, ColumnMeta>) objectInputStream.readObject();

            Collection<ColumnMeta> columnMetas = metastore.values();
            columnMetas.forEach(value -> {
                String backslashRegex = "\\";
                String studyId = value.getName().split(Pattern.quote(backslashRegex))[1];

                if (counts.containsKey(studyId)) {
                    counts.replace(studyId, counts.get(studyId) + 1);
                } else {
                    counts.put(studyId, 1);
                }
            });

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException("Could not load metastore");
        }
        return counts;

    }

    protected static JSONArray updateCountsInJson(String filePath, TreeMap<String, Integer> counts)
            throws FileNotFoundException, IOException, ParseException {
        JSONParser parser = new JSONParser();
        Object fullJson = parser.parse(new FileReader(filePath));
        JSONArray parsedArray = new JSONArray();
        parsedArray.add(fullJson);
        for (Object o : parsedArray) {
            JSONObject studySegment = (JSONObject) o;
            String studyId = (String) studySegment.get("study_identifier");
            System.out.println("Start json string" + studySegment.toJSONString() + " End json string");
            try {
                System.out.println("Study id is" + studyId);
                counts.containsKey(studyId);
            } catch (NullPointerException e) {
                e.printStackTrace();
            }
            if (counts.containsKey(studyId)) {
                studySegment.put("clinical_variable_count", counts.get(studyId));
                System.out.println("Updated counts for " + studyId);
            } else {
                System.out.println("No clinical variables found for identifier " + studyId);
            }
        }
        return parsedArray;
    }
}

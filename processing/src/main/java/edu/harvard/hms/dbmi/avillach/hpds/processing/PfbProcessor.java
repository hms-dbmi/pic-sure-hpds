package edu.harvard.hms.dbmi.avillach.hpds.processing;

import com.google.common.collect.Lists;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.ColumnMeta;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.KeyAndValue;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.PhenoCube;
import edu.harvard.hms.dbmi.avillach.hpds.data.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class PfbProcessor implements HpdsProcessor {

    public static final String PATIENT_ID_FIELD_NAME = "patient_id";
    private final int ID_BATCH_SIZE;
    private final AbstractProcessor abstractProcessor;

    private Logger log = LoggerFactory.getLogger(PfbProcessor.class);


    public PfbProcessor(AbstractProcessor abstractProcessor) {
        this.abstractProcessor = abstractProcessor;
        ID_BATCH_SIZE = Integer.parseInt(System.getProperty("ID_BATCH_SIZE", "0"));
    }

    @Override
    public String[] getHeaderRow(Query query) {
        String[] header = new String[query.getFields().size()+1];
        header[0] = PATIENT_ID_FIELD_NAME;
        System.arraycopy(query.getFields().toArray(), 0, header, 1, query.getFields().size());
        return header;
    }

    @Override
    public void runQuery(Query query, AsyncResult result) {
        Set<Integer> idList = abstractProcessor.getPatientSubsetForQuery(query);
        log.info("Processing " + idList.size() + " rows for result " + result.getId());
        Lists.partition(new ArrayList<>(idList), ID_BATCH_SIZE).stream()
                .forEach(patientIds -> {
                    Map<String, Map<Integer, String>> pathToPatientToValueMap = buildResult(result, query, new TreeSet<>(patientIds));
                    List<String[]> fieldValuesPerPatient = patientIds.stream().map(patientId -> {
                        return Arrays.stream(getHeaderRow(query)).map(field -> {
                            if (PATIENT_ID_FIELD_NAME.equals(field)) {
                                return patientId.toString();
                            } else {
                                return pathToPatientToValueMap.get(field).get(patientId);
                            }
                        }).toArray(String[]::new);
                    }).collect(Collectors.toList());
                    result.appendResults(fieldValuesPerPatient);
                });
    }

    private Map<String, Map<Integer, String>> buildResult(AsyncResult result, Query query, TreeSet<Integer> ids) {
        ConcurrentHashMap<String, Map<Integer, String>> pathToPatientToValueMap = new ConcurrentHashMap<>();
        List<ColumnMeta> columns = query.getFields().stream()
                .map(abstractProcessor.getDictionary()::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        List<String> paths = columns.stream()
                .map(ColumnMeta::getName)
                .collect(Collectors.toList());
        int columnCount = paths.size() + 1;

        ArrayList<Integer> columnIndex = abstractProcessor.useResidentCubesFirst(paths, columnCount);
        ResultStore results = new ResultStore(result.getId(), columns, ids);

        // todo: investigate if the parallel stream will thrash the cache if the number of executors is > number of resident cubes
        columnIndex.parallelStream().forEach((columnId)->{
            String columnPath = paths.get(columnId-1);
            Map<Integer, String> patientIdToValueMap = processColumn(ids, columnPath);
            pathToPatientToValueMap.put(columnPath, patientIdToValueMap);
        });

        return pathToPatientToValueMap;
    }

    private Map<Integer, String> processColumn(TreeSet<Integer> patientIds, String path) {

        Map<Integer, String> patientIdToValueMap = new HashMap<>();
        PhenoCube<?> cube = abstractProcessor.getCube(path);

        KeyAndValue<?>[] cubeValues = cube.sortedByKey();

        int idPointer = 0;
        for(int patientId : patientIds) {
            while(idPointer < cubeValues.length) {
                int key = cubeValues[idPointer].getKey();
                if(key < patientId) {
                    idPointer++;
                } else if(key == patientId){
                    String value = getResultField(cube, cubeValues, idPointer);
                    patientIdToValueMap.put(patientId, value);
                    idPointer++;
                    break;
                } else {
                    break;
                }
            }
        }
        return patientIdToValueMap;
    }

    private String getResultField(PhenoCube<?> cube, KeyAndValue<?>[] cubeValues,
                                 int idPointer) {
        Comparable<?> value = cubeValues[idPointer].getValue();
        return value.toString();
    }
}

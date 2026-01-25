package edu.harvard.hms.dbmi.avillach.hpds.processing;

import com.google.common.collect.Lists;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.ColumnMeta;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.KeyAndValue;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.PhenoCube;
import edu.harvard.hms.dbmi.avillach.hpds.data.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Component
public class MultiValueQueryProcessor implements HpdsProcessor {

    public static final String PATIENT_ID_FIELD_NAME = "patient_id";
    private final int idBatchSize;
    private final AbstractProcessor abstractProcessor;

    private Logger log = LoggerFactory.getLogger(MultiValueQueryProcessor.class);


    @Autowired
    public MultiValueQueryProcessor(AbstractProcessor abstractProcessor, @Value("${ID_BATCH_SIZE:0}") int idBatchSize) {
        this.abstractProcessor = abstractProcessor;
        this.idBatchSize = idBatchSize;
    }

    @Override
    public String[] getHeaderRow(Query query) {
        String[] header = new String[query.getFields().size() + 1];
        header[0] = PATIENT_ID_FIELD_NAME;
        System.arraycopy(query.getFields().toArray(), 0, header, 1, query.getFields().size());
        return header;
    }

    @Override
    public void runQuery(Query query, AsyncResult result) {
        Set<Integer> idList = abstractProcessor.getPatientSubsetForQuery(query);
        log.info("Processing " + idList.size() + " rows for result " + result.getId());
        Lists.partition(new ArrayList<>(idList), idBatchSize).stream().forEach(patientIds -> {
            Map<String, Map<Integer, List<KeyAndValue<?>>>> pathToPatientToValueMap = buildResult(result, query, new TreeSet<>(patientIds));
            List<List<List<KeyAndValue<?>>>> fieldValuesPerPatient = patientIds.stream().map(patientId -> {
                List<List<KeyAndValue<?>>> objectStream = Arrays.stream(getHeaderRow(query)).map(field -> {
                    List<KeyAndValue<?>> keyAndValues = getKeyAndValues(patientId, field, pathToPatientToValueMap);
                    return keyAndValues;
                }).collect(Collectors.toList());
                return objectStream;
            }).collect(Collectors.toList());
            result.appendMultiValueResults(fieldValuesPerPatient);
        });
        result.closeWriter();
    }

    private static List<KeyAndValue<?>> getKeyAndValues(
        Integer patientId, String field, Map<String, Map<Integer, List<KeyAndValue<?>>>> pathToPatientToValueMap
    ) {
        if (PATIENT_ID_FIELD_NAME.equals(field)) {
            KeyAndValue<?> keyAndValue = new KeyAndValue<>(0, patientId.toString());
            return List.of(keyAndValue);
        } else {
            List<KeyAndValue<?>> keyAndValues = pathToPatientToValueMap.get(field).get(patientId);
            return keyAndValues;
        }
    }

    private Map<String, Map<Integer, List<KeyAndValue<?>>>> buildResult(AsyncResult result, Query query, TreeSet<Integer> ids) {
        ConcurrentHashMap<String, Map<Integer, List<KeyAndValue<?>>>> pathToPatientToValueMap = new ConcurrentHashMap<>();
        List<ColumnMeta> columns =
            query.getFields().stream().map(abstractProcessor.getDictionary()::get).filter(Objects::nonNull).collect(Collectors.toList());
        List<String> paths = columns.stream().map(ColumnMeta::getName).collect(Collectors.toList());
        int columnCount = paths.size() + 1;

        ArrayList<Integer> columnIndex = abstractProcessor.useResidentCubesFirst(paths, columnCount);

        // todo: investigate if the parallel stream will thrash the cache if the number of executors is > number of resident cubes
        columnIndex.parallelStream().forEach((columnId) -> {
            String columnPath = paths.get(columnId - 1);
            Map<Integer, List<KeyAndValue<?>>> patientIdToValueMap = processColumn(ids, columnPath);
            pathToPatientToValueMap.put(columnPath, patientIdToValueMap);
        });

        return pathToPatientToValueMap;
    }

    private Map<Integer, List<KeyAndValue<?>>> processColumn(TreeSet<Integer> patientIds, String path) {

        Map<Integer, List<KeyAndValue<?>>> patientIdToValueMap = new HashMap<>();
        PhenoCube<?> cube = abstractProcessor.getCube(path);

        KeyAndValue<?>[] cubeValues = cube.sortedByKey();

        int idPointer = 0;
        for (int patientId : patientIds) {
            while (idPointer < cubeValues.length) {
                int key = cubeValues[idPointer].getKey();
                if (key < patientId) {
                    idPointer++;
                } else if (key == patientId) {
                    KeyAndValue<?> value = cubeValues[idPointer];
                    patientIdToValueMap.computeIfAbsent(patientId, k -> new ArrayList<>()).add(value);
                    idPointer++;
                } else {
                    break;
                }
            }
        }
        return patientIdToValueMap;
    }
}

package edu.harvard.hms.dbmi.avillach.hpds.processing.v3;

import com.google.common.collect.Lists;
import edu.harvard.hms.dbmi.avillach.hpds.data.genotype.VariableVariantMasks;
import edu.harvard.hms.dbmi.avillach.hpds.data.genotype.caching.VariantBucketHolder;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.ColumnMeta;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.KeyAndValue;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.PhenoCube;
import edu.harvard.hms.dbmi.avillach.hpds.data.query.v3.Query;
import edu.harvard.hms.dbmi.avillach.hpds.processing.ResultStore;
import edu.harvard.hms.dbmi.avillach.hpds.processing.VariantUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

/**
 * This class handles DATAFRAME export queries for HPDS.
 * @author nchu
 *
 */
@Component
public class QueryV3Processor implements HpdsV3Processor {
 
	private static final byte[] EMPTY_STRING_BYTES = "".getBytes();
	private Logger log = LoggerFactory.getLogger(QueryV3Processor.class);

	private final String ID_CUBE_NAME;
	private final int ID_BATCH_SIZE;

	private final QueryExecutor queryExecutor;

	@Autowired
	public QueryV3Processor(QueryExecutor queryExecutor) {
		this.queryExecutor = queryExecutor;
		ID_BATCH_SIZE = Integer.parseInt(System.getProperty("ID_BATCH_SIZE", "0"));
		ID_CUBE_NAME = System.getProperty("ID_CUBE_NAME", "NONE");
	}
	
	@Override
	public String[] getHeaderRow(Query query) {
		String[] header = new String[query.select().size()+1];
		header[0] = "Patient ID";
		System.arraycopy(query.select().toArray(), 0, header, 1, query.select().size());
		return header;
	}

	public void runQuery(Query query, AsyncResult result) {
		Set<Integer> idList = queryExecutor.getPatientSubsetForQuery(query);
		log.info("Processing " + idList.size() + " rows for result " + result.getId());
		Lists.partition(new ArrayList<>(idList), ID_BATCH_SIZE).parallelStream()
			.map(list -> buildResult(result, query, new TreeSet<>(list)))
			.sequential()
			.forEach(result::appendResultStore);
	}

	
	private ResultStore buildResult(AsyncResult result, Query query, TreeSet<Integer> ids) {
		List<ColumnMeta> columns = query.select().stream()
			.map(queryExecutor.getDictionary()::get)
			.filter(Objects::nonNull)
			.collect(Collectors.toList());
		List<String> paths = columns.stream()
			.map(ColumnMeta::getName)
			.collect(Collectors.toList());
		int columnCount = paths.size() + 1;

		ArrayList<Integer> columnIndex = queryExecutor.useResidentCubesFirst(paths, columnCount);
		ResultStore results = new ResultStore(result.getId(), columns, ids);

		columnIndex.parallelStream().forEach((column)->{
			clearColumn(paths, ids, results, column);
			processColumn(paths, ids, results, column);
		});

		return results;
	}

	private void clearColumn(List<String> paths, TreeSet<Integer> ids, ResultStore results, Integer x) {
		String path = paths.get(x-1);
		if(VariantUtils.pathIsVariantSpec(path)) {
			ByteBuffer doubleBuffer = ByteBuffer.allocate(Double.BYTES);
			int idInSubsetPointer = 0;
			for(int id : ids) {
				writeVariantNullResultField(results, x, doubleBuffer, idInSubsetPointer);
				idInSubsetPointer++;
			}
		} else {
			PhenoCube<?> cube = queryExecutor.getCube(path);
			ByteBuffer doubleBuffer = ByteBuffer.allocate(Double.BYTES);
			int idInSubsetPointer = 0;
			for(int id : ids) {
				writeNullResultField(results, x, cube, doubleBuffer, idInSubsetPointer);
				idInSubsetPointer++;
			}
		}
	}

	private void processColumn(List<String> paths, TreeSet<Integer> ids, ResultStore results,
			Integer x) {
		String path = paths.get(x-1);
		if(VariantUtils.pathIsVariantSpec(path)) {
			// todo: confirm this entire if block is even used. I don't think it is
			Optional<VariableVariantMasks> masks = queryExecutor.getMasks(path, new VariantBucketHolder<>());
			List<String> patientIds = queryExecutor.getPatientIds();
			int idPointer = 0;

			ByteBuffer doubleBuffer = ByteBuffer.allocate(Double.BYTES);
			int idInSubsetPointer = 0;
			for(int id : ids) {
				while(idPointer < patientIds.size()) {
					int key = Integer.parseInt(patientIds.get(idPointer));
					if(key < id) {
						idPointer++;
					} else if(key == id){
						idPointer = writeVariantResultField(results, x, masks, idPointer, idInSubsetPointer);
						break;
					} else {
						writeVariantNullResultField(results, x, doubleBuffer, idInSubsetPointer);
						break;
					}
				}
				idInSubsetPointer++;
			}
		}else {
			PhenoCube<?> cube = queryExecutor.getCube(path);

			KeyAndValue<?>[] cubeValues = cube.sortedByKey();

			int idPointer = 0;

			ByteBuffer doubleBuffer = ByteBuffer.allocate(Double.BYTES);
			int idInSubsetPointer = 0;
			for(int id : ids) {
				while(idPointer < cubeValues.length) {
					int key = cubeValues[idPointer].getKey();
					if(key < id) {
						idPointer++;
					} else if(key == id){
						idPointer = writeResultField(results, x, cube, cubeValues, idPointer, doubleBuffer,
								idInSubsetPointer);
						break;
					} else {
						writeNullResultField(results, x, cube, doubleBuffer, idInSubsetPointer);
						break;
					}
				}
				idInSubsetPointer++;
			}
		}
	}

	private void writeVariantNullResultField(ResultStore results, Integer x, ByteBuffer doubleBuffer,
			int idInSubsetPointer) {
		byte[] valueBuffer = null;
		valueBuffer = EMPTY_STRING_BYTES;
		results.writeField(x,idInSubsetPointer, valueBuffer);
	}

	private int writeVariantResultField(ResultStore results, Integer x, Optional<VariableVariantMasks> variantMasks, int idPointer,
			int idInSubsetPointer) {
		byte[] valueBuffer = variantMasks.map(masks -> {
			if(masks.heterozygousMask != null && masks.heterozygousMask.testBit(idPointer)) {
				return "0/1".getBytes();
			} else if(masks.homozygousMask != null && masks.homozygousMask.testBit(idPointer)) {
				return "1/1".getBytes();
			}else {
				return "0/0".getBytes();
			}
		}).orElse("".getBytes());
		results.writeField(x,idInSubsetPointer, valueBuffer);
		return idPointer;
	}

	private int writeResultField(ResultStore results, Integer x, PhenoCube<?> cube, KeyAndValue<?>[] cubeValues,
			int idPointer, ByteBuffer doubleBuffer, int idInSubsetPointer) {
		byte[] valueBuffer;
		Comparable<?> value = cubeValues[idPointer++].getValue();
		if(cube.isStringType()) {
			valueBuffer = value.toString().getBytes();
		}else {
			valueBuffer = doubleBuffer.putDouble((Double)value).array();
			doubleBuffer.clear();
		}
		results.writeField(x,idInSubsetPointer, valueBuffer);
		return idPointer;
	}

	/**
	 * Correctly handle null records. A numerical value should be a NaN if it is missing to distinguish from a zero. A
	 * String based value(categorical) should be empty instead of null because the word null might be a valid value.
	 * 
	 * @param results
	 * @param x
	 * @param cube
	 * @param doubleBuffer
	 * @param idInSubsetPointer
	 */
	private void writeNullResultField(ResultStore results, Integer x, PhenoCube<?> cube, ByteBuffer doubleBuffer, int idInSubsetPointer) {
		byte[] valueBuffer = null;
		if(cube.isStringType()) {
			valueBuffer = EMPTY_STRING_BYTES;
		}else {
			Double nullDouble = Double.NaN;
			valueBuffer = doubleBuffer.putDouble(nullDouble).array();
			doubleBuffer.clear();
		}
		results.writeField(x,idInSubsetPointer, valueBuffer);
	}
}

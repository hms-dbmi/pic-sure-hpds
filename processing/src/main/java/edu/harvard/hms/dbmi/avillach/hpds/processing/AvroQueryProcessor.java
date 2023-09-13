package edu.harvard.hms.dbmi.avillach.hpds.processing;

import edu.harvard.hms.dbmi.avillach.hpds.data.genotype.VariantMasks;
import edu.harvard.hms.dbmi.avillach.hpds.data.genotype.caching.VariantBucketHolder;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.ColumnMeta;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.KeyAndValue;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.PhenoCube;
import edu.harvard.hms.dbmi.avillach.hpds.data.query.Query;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * This class handles Avro export queries for HPDS.
 * @author ramari16
 *
 */
@Component
public class AvroQueryProcessor {

	private static final byte[] EMPTY_STRING_BYTES = "".getBytes();
	private Logger log = LoggerFactory.getLogger(AvroQueryProcessor.class);

	private final String ID_CUBE_NAME;
	private final int ID_BATCH_SIZE;

	private final AbstractProcessor abstractProcessor;

	@Autowired
	public AvroQueryProcessor(AbstractProcessor abstractProcessor) {
		this.abstractProcessor = abstractProcessor;
		ID_BATCH_SIZE = Integer.parseInt(System.getProperty("ID_BATCH_SIZE", "0"));
		ID_CUBE_NAME = System.getProperty("ID_CUBE_NAME", "NONE");
	}

	public String[] getHeaderRow(Query query) {
		String[] header = new String[query.getFields().size()+1];
		header[0] = "Patient ID";
		System.arraycopy(query.getFields().toArray(), 0, header, 1, query.getFields().size());
		return header;
	}

	public Schema generateSchema(Query query) {
		SchemaBuilder.FieldAssembler<Schema> record = SchemaBuilder.record("pfb")
				.namespace("edu.harvard.hms.dbmi.avillach")
				.fields();
		query.getFields().stream()
				.map(abstractProcessor.getDictionary()::get)
				.filter(Objects::nonNull)
				.forEach(columnMeta -> {
					if (columnMeta.isCategorical()) {
						record.optionalString(getValidAvroName(columnMeta.getName()));
					} else {
						record.optionalDouble(getValidAvroName(columnMeta.getName()));
					}
				});
		record.requiredInt("patientId");
		return record.endRecord();
	}

	private String getValidAvroName(String string) {
		return string.replaceAll("\\\\", "_").replaceAll(" ", "_");
	}

	public String runQuery(Query query) throws IOException {
		TreeSet<Integer> idList = abstractProcessor.getPatientSubsetForQuery(query);
		log.info("Processing " + idList.size() + " rows for result");
		Schema schema = generateSchema(query);
		GenericRecord[] genericRecords = buildResult(query, idList, schema);


		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);

		File file = new File("/tmp/test.avro");
		dataFileWriter.create(schema, file);
		for (GenericRecord record : genericRecords) {
			dataFileWriter.append(record);
		}
		dataFileWriter.close();
		return  Files.readString(Path.of("/tmp/test.avro"), StandardCharsets.ISO_8859_1);
	}

	
	private GenericRecord[] buildResult(Query query, TreeSet<Integer> ids, Schema schema) {
		List<ColumnMeta> columns = query.getFields().stream()
			.map(abstractProcessor.getDictionary()::get)
			.filter(Objects::nonNull)
			.collect(Collectors.toList());
		List<String> paths = columns.stream()
			.map(ColumnMeta::getName)
			.collect(Collectors.toList());
		int columnCount = paths.size() + 1;

		ArrayList<Integer> columnIndex = abstractProcessor.useResidentCubesFirst(paths, columnCount);

		GenericRecord[] allRecords = ids.stream().map(id -> {
			GenericData.Record record = new GenericData.Record(schema);
			record.put("patientId", id);
			return record;
		}).toArray(GenericRecord[]::new);

		Integer[] patientIds = ids.toArray(new Integer[]{});

		columnIndex.parallelStream().forEach((column)->{
			// todo: this is only pheno fields right now
			PhenoCube<?> cube = abstractProcessor.getCube(paths.get(column - 1));

			KeyAndValue<?>[] cubeValues = cube.sortedByKey();

			int cubeIdPointer = 0;
			for (int patientRow = 0; patientRow < ids.size(); patientRow++) {
				int patientId = patientIds[patientRow];
				while(cubeIdPointer < cubeValues.length) {
					KeyAndValue<?> cubeKeyValue = cubeValues[cubeIdPointer];
					int key = cubeKeyValue.getKey();
					if(key < patientId) {
						cubeIdPointer++;
					} else if(key == patientId){
						Comparable<?> value = cubeKeyValue.getValue();
						GenericRecord patientRecord = allRecords[patientRow];
						if(cube.isStringType()) {
							patientRecord.put(getValidAvroName(columns.get(column - 1).getName()), value.toString());
						}else {
							patientRecord.put(getValidAvroName(columns.get(column - 1).getName()), (Double)value);
						}
						cubeIdPointer++;
						break;
					} else {
						// no value found
						break;
					}
				}
			}
		});

		return allRecords;
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
			PhenoCube<?> cube = abstractProcessor.getCube(path);
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
			VariantMasks masks = abstractProcessor.getMasks(path, new VariantBucketHolder<VariantMasks>());
			String[] patientIds = abstractProcessor.getPatientIds();
			int idPointer = 0;

			ByteBuffer doubleBuffer = ByteBuffer.allocate(Double.BYTES);
			int idInSubsetPointer = 0;
			for(int id : ids) {
				while(idPointer < patientIds.length) {
					int key = Integer.parseInt(patientIds[idPointer]);
					if(key < id) {
						idPointer++;
					} else if(key == id){
						idPointer = writeVariantResultField(results, x, masks, idPointer, doubleBuffer,
								idInSubsetPointer);
						break;
					} else {
						writeVariantNullResultField(results, x, doubleBuffer, idInSubsetPointer);
						break;
					}
				}
				idInSubsetPointer++;
			}
		}else {
			PhenoCube<?> cube = abstractProcessor.getCube(path);

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

	private int writeVariantResultField(ResultStore results, Integer x, VariantMasks masks, int idPointer,
			ByteBuffer doubleBuffer, int idInSubsetPointer) {
		byte[] valueBuffer;
		if(masks.heterozygousMask != null && masks.heterozygousMask.testBit(idPointer)) {
			valueBuffer = "0/1".getBytes();
		}else if(masks.homozygousMask != null && masks.homozygousMask.testBit(idPointer)) {
			valueBuffer = "1/1".getBytes();
		}else {
			valueBuffer = "0/0".getBytes();
		}
		valueBuffer = masks.toString().getBytes();
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

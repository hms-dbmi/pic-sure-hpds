package edu.harvard.hms.dbmi.avillach.hpds.processing.io;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.Codec;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.stream.Collectors;

public class PfbWriter implements ResultWriter {

    private Logger log = LoggerFactory.getLogger(PfbWriter.class);

    private final Schema metadataSchema;
    private final Schema nodeSchema;
    private SchemaBuilder.FieldAssembler<Schema> entityFieldAssembler;

    private List<String> fields;
    private DataFileWriter<GenericRecord> dataFileWriter;
    private File file;
    private Schema entitySchema;
    private Schema patientDataSchema;

    public PfbWriter(File tempFile) {
        file = tempFile;
        entityFieldAssembler = SchemaBuilder.record("entity")
                .namespace("edu.harvard.dbmi")
                .fields();

        SchemaBuilder.FieldAssembler<Schema> nodeRecord = SchemaBuilder.record("nodes")
                .fields()
                .requiredString("name")
                .nullableString("ontology_reference", "null")
                .name("values").type(SchemaBuilder.map().values(SchemaBuilder.nullable().stringType())).noDefault();
        nodeSchema = nodeRecord.endRecord();

        SchemaBuilder.FieldAssembler<Schema> metadataRecord = SchemaBuilder.record("metadata")
                .fields();
        metadataRecord.requiredString("misc");
        metadataRecord = metadataRecord.name("nodes").type(SchemaBuilder.array().items(nodeSchema)).noDefault();
        metadataSchema = metadataRecord.endRecord();
    }

    @Override
    public void writeHeader(String[] data) {
        fields = Arrays.stream(data.clone()).map(this::formatFieldName).collect(Collectors.toList());
        SchemaBuilder.FieldAssembler<Schema> patientRecords = SchemaBuilder.record("patientData")
                .fields();

        fields.forEach(field -> patientRecords.nullableString(field, "null"));
        patientDataSchema = patientRecords.endRecord();

        Schema objectSchema = Schema.createUnion(metadataSchema, patientDataSchema);

        entityFieldAssembler = entityFieldAssembler.name("object").type(objectSchema).noDefault();
        entityFieldAssembler.nullableString("id", "null");
        entityFieldAssembler.requiredString("name");
        entitySchema = entityFieldAssembler.endRecord();

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(entitySchema);
        dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
        try {
            log.info("Creating temp avro file at " + file.getAbsoluteFile());
            dataFileWriter.setCodec(CodecFactory.deflateCodec(5));
            dataFileWriter.create(entitySchema, file);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        writeMetadata();
    }

    protected String formatFieldName(String s) {
        String formattedFieldName = s.replaceAll("\\W", "_");
        if (Character.isDigit(formattedFieldName.charAt(0))) {
            return "_" + formattedFieldName;
        }
        return formattedFieldName;
    }

    private void writeMetadata() {
        GenericRecord entityRecord = new GenericData.Record(entitySchema);

        List<GenericRecord> nodeList = new ArrayList<>();
        for (String field : fields) {
            GenericRecord nodeData = new GenericData.Record(nodeSchema);
            nodeData.put("name", field);
            nodeData.put("ontology_reference", "");
            nodeData.put("values", Map.of());
            nodeList.add(nodeData);
        }
        GenericRecord metadata = new GenericData.Record(metadataSchema);
        metadata.put("misc", "");
        metadata.put("nodes", nodeList);


        entityRecord.put("object", metadata);
        entityRecord.put("name", "metadata");
        entityRecord.put("id", "null");

        try {
            dataFileWriter.append(entityRecord);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void writeEntity(Collection<String[]> entities) {
        entities.forEach(entity -> {
            if (entity.length != fields.size()) {
                throw new IllegalArgumentException("Entity length much match the number of fields in this document");
            }
            GenericRecord patientData = new GenericData.Record(patientDataSchema);
            for(int i = 0; i < fields.size(); i++) {
                patientData.put(fields.get(i), entity[i]);
            }

            GenericRecord entityRecord = new GenericData.Record(entitySchema);
            entityRecord.put("object", patientData);
            entityRecord.put("name", "patientData");
            entityRecord.put("id", "192035");

            try {
                dataFileWriter.append(entityRecord);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    @Override
    public void close() {
        try {
            dataFileWriter.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public File getFile() {
        return file;
    }
}

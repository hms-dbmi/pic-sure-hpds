package edu.harvard.hms.dbmi.avillach.hpds.processing;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;

public class PFBWriter {

    public Schema generateSchema() {
        SchemaBuilder.FieldAssembler<Schema> record = SchemaBuilder.record("pfb")
                .namespace("edu.harvard.hms.dbmi.avillach")
                .fields();
        record.requiredInt("patientId");
        record.requiredString("parentConsent");
        record.requiredString("topmedConsent");
        record.requiredString("consents");
        return record.endRecord();
    }

    public void write() throws IOException {
        Schema schema = generateSchema();
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
        File file = new File("/tmp/test.avro");
        dataFileWriter.create(schema, file);
        GenericRecord row = new GenericData.Record(schema);
        row.put("patientId", 1);
        row.put("parentConsent", "/abc/123/");
        row.put("topmedConsent", "/def/456/");
        row.put("consents", "phs000001");
        dataFileWriter.append(row);
        dataFileWriter.close();
    }

    public static void main(String[] args) throws IOException {
        new PFBWriter().write();
    }
}

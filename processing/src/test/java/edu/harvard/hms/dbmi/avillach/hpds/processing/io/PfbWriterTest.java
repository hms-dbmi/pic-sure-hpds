package edu.harvard.hms.dbmi.avillach.hpds.processing.io;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;


public class PfbWriterTest {

    @Test
    public void writeValidPFB() {
        PfbWriter pfbWriter = new PfbWriter(new File("target/test-result.avro"));

        pfbWriter.writeHeader(new String[] {"\\demographics\\age\\", "\\phs123\\stroke\\"});
        pfbWriter.writeEntity(List.of(new String[]{"80", "Y"},
                new String[]{"70", "N"},
                new String[]{"75", null}
        ));
        pfbWriter.close();
        // todo: validate this programatically
    }

    @Test
    public void formatFieldName_spacesAndBackslashes_replacedWithUnderscore() {
        PfbWriter pfbWriter = new PfbWriter(new File("target/test-result.avro"));
        String formattedName = pfbWriter.formatFieldName("\\Topmed Study Accession with Subject ID\\\\");
        assertEquals("_Topmed_Study_Accession_with_Subject_ID__", formattedName);
    }

    @Test
    public void formatFieldName_startsWithDigit_prependUnderscore() {
        PfbWriter pfbWriter = new PfbWriter(new File("target/test-result.avro"));
        String formattedName = pfbWriter.formatFieldName("123Topmed Study Accession with Subject ID\\\\");
        assertEquals("_123Topmed_Study_Accession_with_Subject_ID__", formattedName);
    }

    @Test
    public void formatFieldName_randomGarbage_replaceWithUnderscore() {
        PfbWriter pfbWriter = new PfbWriter(new File("target/test-result.avro"));
        String formattedName = pfbWriter.formatFieldName("$$$my garbage @vro var!able nam#");
        assertEquals("___my_garbage__vro_var_able_nam_", formattedName);
    }
}
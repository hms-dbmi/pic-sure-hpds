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
                new String[]{"75", "N"}
        ));
        pfbWriter.close();
        // todo: validate this programatically
    }
}
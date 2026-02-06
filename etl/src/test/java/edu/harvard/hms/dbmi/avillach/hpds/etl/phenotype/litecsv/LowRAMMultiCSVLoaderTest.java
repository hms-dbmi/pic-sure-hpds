package edu.harvard.hms.dbmi.avillach.hpds.etl.phenotype.litecsv;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class LowRAMMultiCSVLoaderTest {

    @Test
    void shouldFilterOutNonCSVs(@TempDir File testDir) throws IOException {
        File outputDir = new File(testDir, "output");
        outputDir.mkdirs();
        File inputDir = new File(testDir, "input");
        inputDir.mkdirs();

        Path txtPath = Path.of(inputDir.getAbsolutePath(), "test.txt");
        RandomAccessFile file = new RandomAccessFile(txtPath.toString(), "rw");
        file.setLength(6L * 1024);
        file.close();

        LowRAMMultiCSVLoader subject = new LowRAMMultiCSVLoader(
            inputDir.getAbsolutePath(), outputDir.getAbsolutePath() + "/", false, 5D, null
        );
        int actual = subject.processCSVsFromHPDSDir(5D);

        assertEquals(0, actual);
        // No shard directories should be created for non-CSV files
        File[] shardDirs = outputDir.listFiles(File::isDirectory);
        assertNotNull(shardDirs);
        assertEquals(0, shardDirs.length);
    }

    @Test
    void deriveShardName_basicFilename() {
        assertEquals("shard_study1", LowRAMMultiCSVLoader.deriveShardName("study1.csv"));
    }

    @Test
    void deriveShardName_uppercaseExtension() {
        assertEquals("shard_study1", LowRAMMultiCSVLoader.deriveShardName("study1.CSV"));
    }

    @Test
    void deriveShardName_noExtension() {
        assertEquals("shard_study1", LowRAMMultiCSVLoader.deriveShardName("study1"));
    }

    @Test
    void deriveShardName_specialCharacters_sanitized() {
        assertEquals("shard_my_study_1", LowRAMMultiCSVLoader.deriveShardName("my study 1.csv"));
        assertEquals("shard_study_v2_0", LowRAMMultiCSVLoader.deriveShardName("study.v2.0.csv"));
        assertEquals("shard_test-file_name", LowRAMMultiCSVLoader.deriveShardName("test-file name.csv"));
    }

    @Test
    void deriveShardName_preservesUnderscoresAndHyphens() {
        assertEquals("shard_my-study_name", LowRAMMultiCSVLoader.deriveShardName("my-study_name.csv"));
    }
}

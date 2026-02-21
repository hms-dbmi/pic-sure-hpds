package edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.util;

import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.ColumnMeta;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.KeyAndValue;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.PhenoCube;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

class ColumnMetaBuilderTest {

    @Test
    void testFromPhenoCube_Categorical() {
        // Create categorical PhenoCube with manual sortedByKey setup
        PhenoCube<String> cube = new PhenoCube<>("\\test\\categorical\\", String.class);
        KeyAndValue<String>[] sortedByKey = new KeyAndValue[]{
            new KeyAndValue<>(1, "Value1", null),
            new KeyAndValue<>(1, "Value2", null),
            new KeyAndValue<>(2, "Value1", null),
            new KeyAndValue<>(3, "Value3", null)
        };
        cube.setSortedByKey(sortedByKey);
        cube.setColumnWidth(6); // "Value1".length()

        // Build metadata without width override (should calculate from data)
        ColumnMeta meta = ColumnMetaBuilder.fromPhenoCube(cube);

        // Assertions
        assertEquals("\\test\\categorical\\", meta.getName());
        assertTrue(meta.isCategorical());
        assertEquals(4, meta.getObservationCount()); // 4 observations
        assertEquals(3, meta.getPatientCount()); // 3 unique patients
        assertEquals(6, meta.getWidthInBytes()); // Calculated from "Value1", "Value2", "Value3"

        // Category values should be sorted and unique
        List<String> categoryValues = meta.getCategoryValues();
        assertNotNull(categoryValues);
        assertEquals(3, categoryValues.size());
        assertEquals("Value1", categoryValues.get(0));
        assertEquals("Value2", categoryValues.get(1));
        assertEquals("Value3", categoryValues.get(2));

        // Min/max should be null for categorical
        assertNull(meta.getMin());
        assertNull(meta.getMax());
    }

    @Test
    void testFromPhenoCube_Categorical_WithWidthOverride() {
        PhenoCube<String> cube = new PhenoCube<>("\\test\\concept\\", String.class);
        cube.setSortedByKey(new KeyAndValue[]{
            new KeyAndValue<>(1, "A", null),
            new KeyAndValue<>(2, "B", null)
        });

        // Build with width override
        ColumnMeta meta = ColumnMetaBuilder.fromPhenoCube(cube, 10);

        assertEquals(10, meta.getWidthInBytes()); // Override applied
    }

    @Test
    void testFromPhenoCube_Numeric() {
        // Create numeric PhenoCube
        PhenoCube<Double> cube = new PhenoCube<>("\\test\\numeric\\", Double.class);
        cube.setSortedByKey(new KeyAndValue[]{
            new KeyAndValue<>(1, 10.5, null),
            new KeyAndValue<>(1, 20.3, null),
            new KeyAndValue<>(2, 5.2, null),
            new KeyAndValue<>(3, 15.7, null),
            new KeyAndValue<>(3, 30.9, null)
        });

        // Build metadata
        ColumnMeta meta = ColumnMetaBuilder.fromPhenoCube(cube);

        // Assertions
        assertEquals("\\test\\numeric\\", meta.getName());
        assertFalse(meta.isCategorical());
        assertEquals(5, meta.getObservationCount());
        assertEquals(3, meta.getPatientCount());
        assertEquals(1, meta.getWidthInBytes()); // Always 1 for numeric

        // Min/max should be calculated
        assertNotNull(meta.getMin());
        assertNotNull(meta.getMax());
        assertEquals(5.2, meta.getMin(), 0.001);
        assertEquals(30.9, meta.getMax(), 0.001);

        // Category values should be null for numeric
        assertNull(meta.getCategoryValues());
    }

    @Test
    void testFromPhenoCube_Numeric_WithWidthOverride() {
        PhenoCube<Double> cube = new PhenoCube<>("\\test\\concept\\", Double.class);
        cube.setSortedByKey(new KeyAndValue[]{
            new KeyAndValue<>(1, 1.0, null),
            new KeyAndValue<>(2, 2.0, null)
        });

        // Build with width override (unusual but should work)
        ColumnMeta meta = ColumnMetaBuilder.fromPhenoCube(cube, 5);

        assertEquals(5, meta.getWidthInBytes()); // Override applied
    }

    @Test
    void testFromPhenoCube_EmptyValues() {
        // Test with empty cube
        PhenoCube<String> cube = new PhenoCube<>("\\test\\empty\\", String.class);
        cube.setSortedByKey(new KeyAndValue[0]); // Empty array

        ColumnMeta meta = ColumnMetaBuilder.fromPhenoCube(cube);

        assertEquals("\\test\\empty\\", meta.getName());
        assertEquals(0, meta.getObservationCount());
        assertEquals(0, meta.getPatientCount());
        assertEquals(1, meta.getWidthInBytes()); // Default when no categories
    }

    @Test
    void testFromPhenoCube_NullValues() {
        // Test null-safety in categorical values
        PhenoCube<String> cube = new PhenoCube<>("\\test\\nulls\\", String.class);
        cube.setSortedByKey(new KeyAndValue[]{
            new KeyAndValue<>(1, "Value1", null),
            new KeyAndValue<>(2, null, null), // Should be filtered out
            new KeyAndValue<>(3, "Value2", null)
        });

        ColumnMeta meta = ColumnMetaBuilder.fromPhenoCube(cube);

        // Should only include non-null values
        List<String> categoryValues = meta.getCategoryValues();
        assertNotNull(categoryValues);
        assertEquals(2, categoryValues.size());
        assertFalse(categoryValues.contains(null));
    }

    @Test
    void testWriteColumnMetaCsv(@TempDir Path tempDir) throws IOException {
        // Create test metadata
        TreeMap<String, ColumnMeta> metadataMap = new TreeMap<>();

        ColumnMeta meta1 = new ColumnMeta()
            .setName("\\concept1\\")
            .setWidthInBytes(5)
            .setColumnOffset(0)
            .setCategorical(true)
            .setAllObservationsOffset(100)
            .setAllObservationsLength(200)
            .setObservationCount(10)
            .setPatientCount(5);
        meta1.setCategoryValues(List.of("A", "B", "C"));
        metadataMap.put("\\concept1\\", meta1);

        ColumnMeta meta2 = new ColumnMeta()
            .setName("\\concept2\\")
            .setWidthInBytes(1)
            .setColumnOffset(0)
            .setCategorical(false)
            .setMin(1.0)
            .setMax(10.0)
            .setAllObservationsOffset(200)
            .setAllObservationsLength(300)
            .setObservationCount(20)
            .setPatientCount(8);
        metadataMap.put("\\concept2\\", meta2);

        // Write CSV
        Path csvPath = tempDir.resolve("columnMeta.csv");
        ColumnMetaBuilder.writeColumnMetaCsv(metadataMap, csvPath);

        // Verify file exists and has content
        assertTrue(Files.exists(csvPath));
        List<String> lines = Files.readAllLines(csvPath);
        assertEquals(2, lines.size()); // 2 concepts

        // Verify first line contains concept1 data
        String line1 = lines.get(0);
        assertTrue(line1.contains("\\concept1\\"));
        assertTrue(line1.contains("true")); // isCategorical
        assertTrue(line1.contains("AµBµC")); // Category values with µ delimiter

        // Verify second line contains concept2 data
        String line2 = lines.get(1);
        assertTrue(line2.contains("\\concept2\\"));
        assertTrue(line2.contains("false")); // isCategorical
        assertTrue(line2.contains("1.0")); // min
        assertTrue(line2.contains("10.0")); // max
    }

    @Test
    void testWriteColumnMetaCsv_EmptyMetadata(@TempDir Path tempDir) throws IOException {
        TreeMap<String, ColumnMeta> emptyMap = new TreeMap<>();
        Path csvPath = tempDir.resolve("empty.csv");

        // Should not throw, just create empty file
        assertDoesNotThrow(() -> ColumnMetaBuilder.writeColumnMetaCsv(emptyMap, csvPath));
        assertTrue(Files.exists(csvPath));
    }
}

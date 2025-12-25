package edu.harvard.hms.dbmi.avillach.hpds.ingest.util;

import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.file.Paths;

/**
 * Utility to inspect Parquet file schemas using Apache Arrow Dataset API.
 *
 * Usage: java -cp ... ParquetSchemaInspector /path/to/file.parquet
 */
public class ParquetSchemaInspector {

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: ParquetSchemaInspector <parquet-file-path>");
            System.exit(1);
        }

        String filePath = args[0];
        System.out.println("Inspecting Parquet file: " + filePath);
        System.out.println("=".repeat(80));

        try (BufferAllocator allocator = new RootAllocator()) {
            // Create dataset factory for the Parquet file
            String uri = Paths.get(filePath).toUri().toString();
            ScanOptions options = new ScanOptions(/*batchSize*/ 100);

            try (DatasetFactory datasetFactory = new FileSystemDatasetFactory(
                    allocator,
                    NativeMemoryPool.getDefault(),
                    FileFormat.PARQUET,
                    uri);
                 Dataset dataset = datasetFactory.finish();
                 Scanner scanner = dataset.newScan(options);
                 ArrowReader reader = scanner.scanBatches()) {

                // Read first batch to get schema and sample data
                if (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    Schema schema = root.getSchema();

                    // Print schema
                    System.out.println("\nSchema:");
                    System.out.println("-".repeat(80));
                    for (Field field : schema.getFields()) {
                        System.out.printf("  %-40s %s%n", field.getName(), field.getType());
                    }

                    // Print sample row
                    System.out.println("\n" + "-".repeat(80));
                    System.out.println("Sample Row (first record):");
                    System.out.println("-".repeat(80));

                    if (root.getRowCount() > 0) {
                        for (Field field : schema.getFields()) {
                            Object value = root.getVector(field.getName()).getObject(0);
                            System.out.printf("  %-40s %s%n", field.getName() + ":", value);
                        }
                    } else {
                        System.out.println("  (No data in first batch)");
                    }

                    System.out.println("\n" + "=".repeat(80));
                    System.out.println("Inspection complete");
                } else {
                    System.out.println("  (No batches available - file may be empty)");
                }
            }
        } catch (Exception e) {
            System.err.println("Error inspecting Parquet file: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}

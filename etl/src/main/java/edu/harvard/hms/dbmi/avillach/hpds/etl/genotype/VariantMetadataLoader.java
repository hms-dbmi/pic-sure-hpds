package edu.harvard.hms.dbmi.avillach.hpds.etl.genotype;


import java.io.*;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.harvard.hms.dbmi.avillach.hpds.data.genotype.VariantMetadataIndex;
import edu.harvard.hms.dbmi.avillach.hpds.data.genotype.VariantSpec;

/**
 * 
 * This loader will read in the metadata associated with each variant and build a VariantMetadataIndex that
 * can be used to populate data in the PIC-SURE Varaint Explorer.
 */
public class VariantMetadataLoader {
	
	private static Logger log = LoggerFactory.getLogger(VariantMetadataLoader.class);
	
	private static VariantMetadataIndex metadataIndex;
	
	//fields to allow tests to override default file location
	private static String storagePathForTests = null;
	private static String variantIndexPathForTests = null;

	private static final int
	ANNOTATED_FLAG_COLUMN = 2,
	GZIP_FLAG_COLUMN=3,
	FILE_COLUMN = 0;	
	
	public static void main(String[] args) throws IOException {
		File vcfIndexFile;
		
		log.info(new File(".").getAbsolutePath());
		if(args.length > 0 && new File(args[0]).exists()) {
			log.info("using path from command line");
			vcfIndexFile = new File(args[0]);
			variantIndexPathForTests = args[1];
			storagePathForTests = args[2];
			metadataIndex = new VariantMetadataIndex(storagePathForTests);
		}else {
			metadataIndex = new VariantMetadataIndex();
			vcfIndexFile = new File("/opt/local/hpds/vcfIndex.tsv");
		}
		
		TreeSet<VcfInputFile> vcfFileTree = new TreeSet<>();

		try(CSVParser parser = CSVParser.parse(vcfIndexFile, Charset.forName("UTF-8"), CSVFormat.DEFAULT.withDelimiter('\t').withSkipHeaderRecord(true))) { 
			final boolean[] horribleHeaderSkipFlag = {false}; 
			parser.forEach((CSVRecord r)->{
				if(horribleHeaderSkipFlag[0]) {
					File vcfFileLocal = new File(r.get(FILE_COLUMN)); 
					if(Integer.parseInt(r.get(ANNOTATED_FLAG_COLUMN).trim())==1) {
						VcfInputFile vcfInput = new VcfInputFile(vcfFileLocal, (Integer.parseInt(r.get(GZIP_FLAG_COLUMN).trim())==1));
						vcfFileTree.add(vcfInput);
					}
				}else {
					horribleHeaderSkipFlag[0] = true;
				}
			});
		}
		
		String currentContig = "";
		while( vcfFileTree.size() > 0) {
			
			//find and remove the lowest element
			VcfInputFile vcfInput = vcfFileTree.pollFirst();
			
			//write to disk each time the contig changes
			if(! currentContig.equals(vcfInput.currentContig)) {
				metadataIndex.flush();
			}
			
			currentContig = vcfInput.currentContig;
			metadataIndex.put(vcfInput.currentVariantSpec, vcfInput.currentMetaData);
			
			if(vcfInput.hasNextVariant()) {
				vcfInput.nextVariant();
				vcfFileTree.add(vcfInput);
			} else {
				log.info("Finished processing:  "+ vcfInput.fileName);  
			}
		}
		
		metadataIndex.complete(); 
		
		//store this in a path per contig (or a preset path 
		String binfilePath = variantIndexPathForTests == null ?  VariantMetadataIndex.VARIANT_METADATA_BIN_FILE : variantIndexPathForTests;
		
		try(ObjectOutputStream out = new ObjectOutputStream(new GZIPOutputStream(new FileOutputStream(new File(binfilePath))))){
			out.writeObject(metadataIndex);
			out.flush();
		}
	}
}

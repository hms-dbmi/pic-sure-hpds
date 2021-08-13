package edu.harvard.hms.dbmi.avillach.hpds.etl.genotype;


import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;

import edu.harvard.hms.dbmi.avillach.hpds.data.genotype.VariantMetadataIndex;
import edu.harvard.hms.dbmi.avillach.hpds.data.genotype.VariantSpec;

/**
 * 
 * This loader will read in the metadata associated with each variant and build a VariantMetadataIndex that
 * can be used to populate data in the PIC-SURE Varaint Explorer.
 */
public class VariantMetadataLoader {
	
	private static Logger log = Logger.getLogger(VariantMetadataLoader.class);
	
	private static VariantMetadataIndex metadataIndex;
	
	//fields to allow tests to override default file location
	private static String storagePathForTests = null;
	private static String variantIndexPathForTests = null;
	
	private static final int 
	INFO_COLUMN = 7,
	ANNOTATED_FLAG_COLUMN = 2,
	GZIP_FLAG_COLUMN=3,
	FILE_COLUMN = 0;	
	
	public static void main(String[] args) throws Exception{  
		File vcfIndexFile;
		
		log.info(new File(".").getAbsolutePath());
		if(args.length > 0 && new File(args[0]).exists()) {
			log.info("using path from command line, is this a test");
			vcfIndexFile = new File(args[0]);
			variantIndexPathForTests = args[1];
			storagePathForTests = args[2];
			metadataIndex = new VariantMetadataIndex(storagePathForTests);
		}else {
			metadataIndex = new VariantMetadataIndex();
			vcfIndexFile = new File("/opt/local/hpds/vcfIndex.tsv");
		}
		
		List<VcfInputFile> vcfFiles = new ArrayList<>();

		try(CSVParser parser = CSVParser.parse(vcfIndexFile, Charset.forName("UTF-8"), CSVFormat.DEFAULT.withDelimiter('\t').withSkipHeaderRecord(true))) { 
			final boolean[] horribleHeaderSkipFlag = {false}; 
			parser.forEach((CSVRecord r)->{
				if(horribleHeaderSkipFlag[0]) {
					File vcfFileLocal = new File(r.get(FILE_COLUMN)); 
					if(Integer.parseInt(r.get(ANNOTATED_FLAG_COLUMN).trim())==1) {
						VcfInputFile vcfInput = new VcfInputFile();
						vcfInput.vcfFile = vcfFileLocal;
						vcfInput.gzipped = (Integer.parseInt(r.get(GZIP_FLAG_COLUMN).trim())==1);
						vcfFiles.add(vcfInput);
					}
				}else {
					horribleHeaderSkipFlag[0] = true;
				}
			});
		}
		
		//process each tsv (vcf file) into our set of VariantMetadataIndex files
		vcfFiles.parallelStream().forEach((VcfInputFile vcfFile)->{
			processVCFFile(vcfFile.vcfFile, vcfFile.gzipped); 
		});
		
		
		metadataIndex.complete(); 
		
		//store this in a path per contig (or a preset path 
		String binfilePath = variantIndexPathForTests == null ?  VariantMetadataIndex.VARIANT_METADATA_BIN_FILE : variantIndexPathForTests;
		
		try(ObjectOutputStream out = new ObjectOutputStream(new GZIPOutputStream(new FileOutputStream(new File(binfilePath))))){
			out.writeObject(metadataIndex);
			out.flush();
		}
	}
	 
	public static void processVCFFile(File vcfFile, boolean gzipFlag) {  
		log.info("Processing VCF file:  "+vcfFile.getName());   
		try(Reader reader = new InputStreamReader( 
			gzipFlag ? new GZIPInputStream(new FileInputStream(vcfFile)) : new FileInputStream(vcfFile));
			CSVParser parser = new CSVParser(reader, CSVFormat.DEFAULT.withDelimiter('\t').withSkipHeaderRecord(false))){
			Iterator<CSVRecord> iterator = parser.iterator();   
			while(iterator.hasNext()) { 
			    CSVRecord csvRecord = iterator.next(); 
			    //skip all header rows
		    	if(csvRecord.get(0).startsWith("#")) {  
			    	continue;
			    }
			    
		    	VariantSpec variantSpec = new VariantSpec(csvRecord); 
		    	metadataIndex.put(variantSpec.specNotation(), csvRecord.get(INFO_COLUMN).trim());
			} 	
			log.info("Finished processing:  "+vcfFile.getName());  
		}catch(IOException e) {
			log.error("Error processing VCF file: " + vcfFile.getName(), e);
		}
	}   
	
	private static class VcfInputFile {
		File vcfFile;
		boolean gzipped;
	}
}

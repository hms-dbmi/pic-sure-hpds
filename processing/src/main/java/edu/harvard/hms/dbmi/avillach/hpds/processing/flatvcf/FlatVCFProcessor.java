
package edu.harvard.hms.dbmi.avillach.hpds.processing.flatvcf;

import edu.harvard.hms.dbmi.avillach.hpds.data.genotype.VariantMasks;
import edu.harvard.hms.dbmi.avillach.hpds.data.genotype.VariantMetadataIndex;
import edu.harvard.hms.dbmi.avillach.hpds.data.genotype.VariantSpec;
import edu.harvard.hms.dbmi.avillach.hpds.data.genotype.caching.VariantBucketHolder;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.PhenoCube;
import edu.harvard.hms.dbmi.avillach.hpds.data.query.Query;
import edu.harvard.hms.dbmi.avillach.hpds.exception.NotEnoughMemoryException;
import edu.harvard.hms.dbmi.avillach.hpds.processing.AbstractProcessor;
import edu.harvard.hms.dbmi.avillach.hpds.processing.AsyncResult;
import edu.harvard.hms.dbmi.avillach.hpds.processing.HpdsProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

@Component
public class FlatVCFProcessor implements HpdsProcessor {

	private final VariantMetadataIndex metadataIndex;

	private static Logger log = LoggerFactory.getLogger(FlatVCFProcessor.class);
	
	private final Boolean VCF_EXCERPT_ENABLED;
	private final Boolean AGGREGATE_VCF_EXCERPT_ENABLED;
	private final Boolean VARIANT_LIST_ENABLED;
	private final String ID_CUBE_NAME;
	private final int ID_BATCH_SIZE;
	private final int CACHE_SIZE;

	private final AbstractProcessor abstractProcessor;


	@Autowired
	public FlatVCFProcessor(AbstractProcessor abstractProcessor) {
		this.abstractProcessor = abstractProcessor;
		this.metadataIndex = VariantMetadataIndex.createInstance(VariantMetadataIndex.VARIANT_METADATA_BIN_FILE);

		VCF_EXCERPT_ENABLED = "TRUE".equalsIgnoreCase(System.getProperty("VCF_EXCERPT_ENABLED", "FALSE"));
		//always enable aggregate queries if full queries are permitted.
		AGGREGATE_VCF_EXCERPT_ENABLED = VCF_EXCERPT_ENABLED || "TRUE".equalsIgnoreCase(System.getProperty("AGGREGATE_VCF_EXCERPT_ENABLED", "FALSE"));
		VARIANT_LIST_ENABLED = VCF_EXCERPT_ENABLED || AGGREGATE_VCF_EXCERPT_ENABLED;
		CACHE_SIZE = Integer.parseInt(System.getProperty("CACHE_SIZE", "100"));
		ID_BATCH_SIZE = Integer.parseInt(System.getProperty("ID_BATCH_SIZE", "0"));
		ID_CUBE_NAME = System.getProperty("ID_CUBE_NAME", "NONE");

	}

	public FlatVCFProcessor(boolean isOnlyForTests, AbstractProcessor abstractProcessor)  {
		this.abstractProcessor = abstractProcessor;
		this.metadataIndex = null;

		VCF_EXCERPT_ENABLED = "TRUE".equalsIgnoreCase(System.getProperty("VCF_EXCERPT_ENABLED", "FALSE"));
		//always enable aggregate queries if full queries are permitted.
		AGGREGATE_VCF_EXCERPT_ENABLED = VCF_EXCERPT_ENABLED || "TRUE".equalsIgnoreCase(System.getProperty("AGGREGATE_VCF_EXCERPT_ENABLED", "FALSE"));
		VARIANT_LIST_ENABLED = VCF_EXCERPT_ENABLED || AGGREGATE_VCF_EXCERPT_ENABLED;
		CACHE_SIZE = Integer.parseInt(System.getProperty("CACHE_SIZE", "100"));
		ID_BATCH_SIZE = Integer.parseInt(System.getProperty("ID_BATCH_SIZE", "0"));
		ID_CUBE_NAME = System.getProperty("ID_CUBE_NAME", "NONE");

		if(!isOnlyForTests) {
			throw new IllegalArgumentException("This constructor should never be used outside tests");
		}
	}

	@Override
	public void runQuery(Query query, AsyncResult asyncResult)
			throws NotEnoughMemoryException {
		throw new RuntimeException("Not implemented");
	}

	/**
	 * 
	 *
	 * This should not actually do any filtering based on bitmasks, just INFO columns.
	 * 
	 * @throws IOException
	 */
	public String runVariantListQuery(Query query) throws IOException {
		
		if(!VARIANT_LIST_ENABLED) {
			log.warn("VARIANT_LIST query attempted, but not enabled.");
			return "VARIANT_LIST query type not allowed";
		}
		
		return  Arrays.toString( abstractProcessor.getVariantList(query).toArray());
	}
	
	/**
	 * Process only variantInfoFilters to count the number of variants that would be included in evaluating the query.
	 * 
	 * @throws IOException
	 */
	public int runVariantCount(Query query) throws IOException {
		if(!query.getVariantInfoFilters().isEmpty()) {
			return abstractProcessor.getVariantList(query).size();
		}
		return 0;
	}

	/**
	 *  This method takes a Query input (expected but not validated to be expected result type = VCF_EXCERPT) and
	 *  returns a tab separated string representing a table describing the variants described by the query and the 
	 *  associated zygosities of subjects identified by the query. it includes a header row describing each column.
	 *  The output columns start with the variant description (chromosome, position reference allele, and subsitution),
	 *  continuing with a series of columns describing the Info columns associated with the variant data.  A count 
	 *  of how many subjects in the result set have/do not have comes next, followed by one column per patient.
	 *  The default patientId header value can be overridden by passing the ID_CUBE_NAME environment variable to 
	 *  the java VM.
	 *  
	 *  @param includePatientData whether to include patient specific data
	 *  @return A Tab-separated string with one line per variant and one column per patient (plus variant data columns)
	 * @throws IOException 
	 */
	public void runVcfExcerptQuery(Query query, boolean includePatientData) throws IOException {
		Files.createFile(Path.of("/tmp/output.tsv"));
		FlatVCFWriter writer = new FlatVCFWriter(new FileWriter("/tmp/output.tsv"));

		if(includePatientData && !VCF_EXCERPT_ENABLED) {
			log.warn("VCF_EXCERPT query attempted, but not enabled.");
			writer.complete();
			return;
		} else if (!includePatientData && !AGGREGATE_VCF_EXCERPT_ENABLED) {
			log.warn("AGGREGATE_VCF_EXCERPT query attempted, but not enabled.");
			writer.complete();
			return;
		}
		
		
		log.info("Running VCF Extract query");

		Collection<String> variantList = abstractProcessor.getInfoStore("Gene_with_variant").getAllValues().keys();
		
		log.debug("variantList Size " + variantList.size());

		PhenoCube<String> idCube = null;
		if(!ID_CUBE_NAME.contentEquals("NONE")) {
			idCube = (PhenoCube<String>) abstractProcessor.getCube(ID_CUBE_NAME);
		}

		//Build the header row
		StringBuilder builder = new StringBuilder();

		//5 columns for gene info
		builder.append("CHROM\tPOSITION\tREF\tALT\t");

		//now add the variant metadata column headers
		builder.append(String.join("\t", abstractProcessor.getInfoStoreColumns()));

		//patient count columns
		builder.append("\tPatients with this variant in subset\tPatients with this variant NOT in subset");

		//then one column per patient.  We also need to identify the patient ID and
		// map it to the right index in the bit mask fields.
		TreeSet<Integer> patientSubset = abstractProcessor.getPatientSubsetForQuery(query);
		log.debug("identified " + patientSubset.size() + " patients from query");
		Map<String, Integer> patientIndexMap = new LinkedHashMap<String, Integer>(); //keep a map for quick index lookups
		BigInteger patientMasks = abstractProcessor.createMaskForPatientSet(patientSubset);
		int index = 2; //variant bitmasks are bookended with '11'

		for(String patientId : abstractProcessor.getPatientIds()) {
			Integer idInt = Integer.parseInt(patientId);
			if(patientSubset.contains(idInt)){
				patientIndexMap.put(patientId, index);
				if(includePatientData) {
					if(idCube==null) {
						builder.append("\t").append(patientId);
					} else {
						String value = idCube.getValueForKey(idInt);
						if(value==null) {
							builder.append("\t").append(patientId);
						}else {
							builder.append("\t").append(idCube.getValueForKey(idInt));
						}
					}
				}
			}
			index++;

			if(patientIndexMap.size() >= patientSubset.size()) {
				log.info("Found all " + patientIndexMap.size() + " patient Indices at index " + index);
				break;
			}
		}
		//End of headers
		writer.writeRow(builder.toString());
		VariantBucketHolder<VariantMasks> variantMaskBucketHolder = new VariantBucketHolder<VariantMasks>();

		//loop over the variants identified, and build an output row
		variantList.stream()
			.map(variant -> metadataIndex.findByMultipleVariantSpec(List.of(variant)))
			.filter(Objects::nonNull)
			.flatMap(m -> m.entrySet().stream())
			.map(entry -> createRow(includePatientData, entry, variantMaskBucketHolder, patientMasks, patientIndexMap))
			.forEach(writer::writeRow);

		writer.complete();
	}

	private String createRow(boolean includePatientData, Map.Entry<String, String[]> entry, VariantBucketHolder<VariantMasks> variantMaskBucketHolder, BigInteger patientMasks, Map<String, Integer> patientIndexMap) {
		StringBuilder stringBuilder = new StringBuilder();
		String variantSpec = entry.getKey();
		String[] variantMetadata = entry.getValue();

		String[] variantDataColumns = variantSpec.split(",");
		//4 fixed columns in variant ID (CHROM POSITION REF ALT)
		for(int i = 0; i < 4; i++) {
			if(i > 0) {
				stringBuilder.append("\t");
			}
			// TODO: I don't think this is a rational thought, much less reasonable code
			if(i < variantDataColumns.length) {
				stringBuilder.append(variantDataColumns[i]);
			}
		}
		Map<String,Set<String>> variantColumnMap = new HashMap<>();
		for(String infoColumns : variantMetadata) {
			//data is in a single semi-colon delimited string.
			// e.g.,   key1=value1;key2=value2;....

			String[] metaDataColumns = infoColumns.split(";");

			for(String key : metaDataColumns) {
				String[] keyValue = key.split("=");
				if(keyValue.length == 2 && keyValue[1] != null) {
                    Set<String> existingValues = variantColumnMap.computeIfAbsent(keyValue[0], k -> new HashSet<>());
                    existingValues.add(keyValue[1]);
				}
			}
		}

		//need to make sure columns are pushed out in the right order; use same iterator as headers
		for(String key : abstractProcessor.getInfoStoreColumns()) {
			Set<String> columnMeta = variantColumnMap.get(key);
			if(columnMeta != null) {
				//collect our sets to a single entry
				stringBuilder.append("\t").append(String.join(",", columnMeta));
			} else {
				stringBuilder.append("\tnull");
			}
		}

		VariantMasks masks = abstractProcessor.getMasks(variantSpec, variantMaskBucketHolder);

		//make strings of 000100 so we can just check 'char at'
		//so heterozygous no calls we want, homozygous no calls we don't
		BigInteger heteroMask = masks.heterozygousMask != null? masks.heterozygousMask : masks.heterozygousNoCallMask != null ? masks.heterozygousNoCallMask : null;
		BigInteger homoMask = masks.homozygousMask != null? masks.homozygousMask : null;


		String heteroMaskString = heteroMask != null ? heteroMask.toString(2) : null;
		String homoMaskString = homoMask != null ? homoMask.toString(2) : null;

		// Patient count = (hetero mask | homo mask) & patient mask
		BigInteger heteroOrHomoMask = orNullableMasks(heteroMask, homoMask);
		int patientCount = heteroOrHomoMask == null ? 0 :  (heteroOrHomoMask.and(patientMasks).bitCount() - 4);

		int bitCount = masks.heterozygousMask == null? 0 : (masks.heterozygousMask.bitCount() - 4);
		bitCount += masks.homozygousMask == null? 0 : (masks.homozygousMask.bitCount() - 4);

		//count how many patients have genomic data available
		int patientsWithVariants;
		if(heteroMaskString != null) {
			patientsWithVariants = heteroMaskString.length() - 4;
		} else if (homoMaskString != null ) {
			patientsWithVariants = homoMaskString.length() - 4;
		} else {
			patientsWithVariants = -1;
		}


		// (patients with/total) in subset   \t   (patients with/total) out of subset.
		stringBuilder
			.append("\t").append(patientCount).append("/").append(patientIndexMap.size())
			.append("\t").append(bitCount - patientCount).append("/").append(patientsWithVariants - patientIndexMap.size());

		if (includePatientData) {
			for(Integer patientIndex : patientIndexMap.values()) {
				if(heteroMaskString != null && '1' == heteroMaskString.charAt(patientIndex)) {
					stringBuilder.append("\t0/1");
				}else if(homoMaskString != null && '1' == homoMaskString.charAt(patientIndex)) {
					stringBuilder.append("\t1/1");
				}else {
					stringBuilder.append("\t0/0");
				}
			}
		}

		return stringBuilder.toString();
	}

	private BigInteger orNullableMasks(BigInteger heteroMask, BigInteger homoMask) {
		if (heteroMask != null) {
			if (homoMask != null) {
				return heteroMask.or(homoMask);
			}
			return heteroMask;
		} else {
			return homoMask;
		}
	}

	public String[] getHeaderRow(Query query) {
		return null;
	}
}
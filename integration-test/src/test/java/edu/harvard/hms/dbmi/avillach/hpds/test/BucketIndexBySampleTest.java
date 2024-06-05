package edu.harvard.hms.dbmi.avillach.hpds.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


import edu.harvard.hms.dbmi.avillach.hpds.data.genotype.BucketIndexBySample;
import edu.harvard.hms.dbmi.avillach.hpds.data.genotype.VariantStore;
import edu.harvard.hms.dbmi.avillach.hpds.test.util.BuildIntegrationTestEnvironment;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.springframework.test.util.AssertionErrors.*;

/**
 * These tests are in the ETL project so that we can read in data from disk each time instead of storing binfiles
 * that may become outdated.
 * 
 * the BucketIndexBySample.filterVariantSetForPatientSet method removes variants base on the patent BUCKET MASK;  just because a patient
 * does not have a particular variant doesn't mean it will be filtered out e.g., when a patient has a different variant in the same bucket.
 * 
 * Filtering the specific variants is typically done by the calling function after filtering out the unneeded buckets.
 * 
 * @author nchu
 *
 */
public class BucketIndexBySampleTest {

	private static final String VCF_INDEX_FILE = "./src/test/resources/bucketIndexBySampleTest_vcfIndex.tsv";
	private static final String STORAGE_DIR = "./target/";
	private static final String MERGED_DIR = "./target/merged/";
	
	private static VariantStore variantStore;
	private static BucketIndexBySample bucketIndexBySample;
	
	//Some known variant specs from the input file. 
	//Some known variant specs from the input file.  These have been designed for testing partially overlapping specs
	private static final String spec1 = "chr4,9856624,CAAAAA,C,TVP23A,splice_acceptor_variant";  	private static final String spec1Info = "Gene_with_variant=TVP23A;Variant_consequence_calculated=splice_acceptor_variant;AC=401;AF=8.00719e-02;NS=2504;AN=5008;EAS_AF=3.37000e-02;EUR_AF=4.97000e-02;AFR_AF=1.64100e-01;AMR_AF=3.75000e-02;SAS_AF=7.57000e-02;DP=18352;AA=G|||;VT=SNP";
	private static final String spec2 = "chr4,9856624,CAAA,C,TVP23A,splice_acceptor_variant";		private static final String spec2Info = "Gene_with_variant=TVP23A;Variant_consequence_calculated=splice_acceptor_variant;AC=62;AF=1.23802e-02;NS=2504;AN=5008;EAS_AF=0.00000e+00;EUR_AF=1.00000e-03;AFR_AF=4.54000e-02;AMR_AF=1.40000e-03;SAS_AF=0.00000e+00;DP=18328;AA=T|||;VT=SNP";
	private static final String spec3 = "chr4,9856624,CA,C,TVP23A,splice_acceptor_variant";		private static final String spec3Info = "Gene_with_variant=TVP23A;Variant_consequence_calculated=splice_acceptor_variant;AC=8;AF=1.59744e-03;NS=2504;AN=5008;EAS_AF=0.00000e+00;EUR_AF=0.00000e+00;AFR_AF=6.10000e-03;AMR_AF=0.00000e+00;SAS_AF=0.00000e+00;DP=18519;AA=T|||;VT=SNP";
	private static final String spec4 = "chr4,9856624,C,CA,TVP23A,splice_acceptor_variant";		private static final String spec4Info = "Gene_with_variant=TVP23A;Variant_consequence_calculated=splice_acceptor_variant;AC=75;AF=1.49760e-02;NS=2504;AN=5008;EAS_AF=3.27000e-02;EUR_AF=2.49000e-02;AFR_AF=6.80000e-03;AMR_AF=4.30000e-03;SAS_AF=5.10000e-03;DP=18008;AA=A|||;VT=SNP";
	private static final String spec5 = "chr4,9856624,CAAAAA,CA,TVP23A,splice_acceptor_variant";	private static final String spec5Info = "Gene_with_variant=TVP23A;Variant_consequence_calculated=splice_acceptor_variant;AC=3033;AF=6.05631e-01;NS=2504;AN=5008;EAS_AF=5.23800e-01;EUR_AF=7.54500e-01;AFR_AF=4.28900e-01;AMR_AF=7.82400e-01;SAS_AF=6.50300e-01;DP=20851;VT=INDEL";
	
	private static final String spec6 = "chr21,5032061,A,G,LOC102723996,missense_variant";
	private static final String spec7 = "chr21,5033914,A,G,LOC102723996,missense_variant";
	private static final String spec8 = "chr21,5033988,C,G,LOC102723996,synonymous_variant";
	private static final String spec9 = "chr21,5034028,C,T,LOC102723996,missense_variant";
	private static final String spec10 = "chr21,5102095,C,T,LOC101928576,splice_region_variant";    // patient 9 and 10 are 1/.
	private static final String spec11 = "chr21,5121768,A,G,LOC102724023,missense_variant";   //patient 9 and 10 are 0/.
	private static final String spec12 = "chr21,5121787,C,T,LOC102724023,missense_variant";    //patient 7 is ./.
	
//  ## Patient 1 - NO variants
//	## Patient 2 - ALL variants
//	## Patient 3 - NO CHR 14 variants, ALL CHR 4 variants																		
//	## Patient 4 - ALL CHR 14 variants, NO CHR 4 variants
//	## others mixed
//	patient 5 has spec 1 and 5
//	patient 6 has spec 4 and 5
//
// For no call variants - ./1 1/. count yes, ./0 0/. count NO

	
	//these parameters to the BucketIndexBySample methods are configured by each test
	Set<String>  variantSet;
	List<Integer> patientSet;
	
	@BeforeAll
	public static void initializeBinfile() throws Exception {
		BuildIntegrationTestEnvironment instance = BuildIntegrationTestEnvironment.INSTANCE;
		VariantStore variantStore = VariantStore.readInstance(STORAGE_DIR);
		
		//now use that object to initialize the BucketIndexBySample object
		bucketIndexBySample = new BucketIndexBySample(variantStore, STORAGE_DIR);
//		bucketIndexBySample.printPatientMasks();
	}
	
	@BeforeEach
	public void setUpTest() {
		//start with fresh, empty collections
		variantSet = new HashSet<String>();
		patientSet = new ArrayList<Integer>();
	}
	
	@Test
	public void test_filterVariantSetForPatientSet_noPatients() throws IOException {
		variantSet.add(spec1);
		variantSet.add(spec2);
		variantSet.add(spec3);
		
		Collection<String> filteredVariantSet = bucketIndexBySample.filterVariantSetForPatientSet(variantSet, patientSet);
		
		assertTrue("Empty Patient List should filter out all variants", filteredVariantSet.isEmpty());
	}
	
	@Test
	public void test_filterVariantSetForPatientSet_noVariants() throws IOException {
		patientSet.add(1);
		patientSet.add(2);
		patientSet.add(3);
		
		Collection<String> filteredVariantSet = bucketIndexBySample.filterVariantSetForPatientSet(variantSet, patientSet);
		
		assertTrue("Empty Variant Set should remain empty", filteredVariantSet.isEmpty());
	}
	
	@Test
	public void test_filterVariantSetForPatientSet_VariantsWithoutPatientsLastBucket() throws IOException {
		System.out.println("test_filterVariantSetForPatientSet_VariantsWithoutPatientsLastBucket");
		
		variantSet.add(spec12);
		
		patientSet.add(197506);
		patientSet.add(197508);
		patientSet.add(197509);

		Collection<String> filteredVariantSet = bucketIndexBySample.filterVariantSetForPatientSet(variantSet, patientSet);
		
		assertTrue("Patients should not match any variants in the list", filteredVariantSet.isEmpty());
	}
	
	@Test
	public void test_filterVariantSetForPatientSet_PatientsWithNoVariantsFirstBucket() throws IOException {
		System.out.println("test_filterVariantSetForPatientSet_PatientsWithNoVariantsFirstBucket");
		
		variantSet.add(spec6);
		//variantSet.add(spec7);

		//patientSet.add(202476);
		patientSet.add(202477);
		//patientSet.add(202478);

		Collection<String> filteredVariantSet = bucketIndexBySample.filterVariantSetForPatientSet(variantSet, patientSet);
		
		assertTrue("Patients should not match any variants in the list", filteredVariantSet.isEmpty());
	}
	
	@Test
	public void test_filterVariantSetForPatientSet_allValidLastBucket() throws IOException {
		System.out.println("test_filterVariantSetForPatientSet_allValidLastBucket");

		variantSet.add(spec6);
		variantSet.add(spec7);

		patientSet.add(197506);
		patientSet.add(197508);
		patientSet.add(197509);
		
		Collection<String> filteredVariantSet = bucketIndexBySample.filterVariantSetForPatientSet(variantSet, patientSet);
		
		assertEquals("No variants should be filtered out", (long)3, (long)filteredVariantSet.size());
	}
	
	@Test
	public void test_filterVariantSetForPatientSet_allValidFirstBucket() throws IOException {
		System.out.println("test_filterVariantSetForPatientSet_allValidFirstBucket");
		
		//specs 1-5 are in the last bucket 
		variantSet.add(spec6);
		variantSet.add(spec7);
		variantSet.add(spec8);
		
		patientSet.add(2);
		patientSet.add(3);
		patientSet.add(5);
		patientSet.add(6);
		
		Collection<String> filteredVariantSet = bucketIndexBySample.filterVariantSetForPatientSet(variantSet, patientSet);
		
		assertEquals("No variants should be filtered out", (long)3, (long)filteredVariantSet.size());
	}
	
	@Test
	public void test_filterVariantSetForPatientSet_someValid() throws IOException {
		System.out.println("test_filterVariantSetForPatientSet_someValid");
		
		//specs 1-5 are in the last bucket 
		variantSet.add(spec1);
		variantSet.add(spec6);
		
		patientSet.add(1);
		patientSet.add(3);
		
		Collection<String> filteredVariantSet = bucketIndexBySample.filterVariantSetForPatientSet(variantSet, patientSet);
		
		assertEquals("One variant should be filtered out", (long)1, (long)filteredVariantSet.size());
		assertTrue("Expected variant not found", filteredVariantSet.contains(spec1));
	}
	
	@Test
	public void test_filterVariantSetForPatientSet_allValidDifferentPatients() throws IOException {
		System.out.println("test_filterVariantSetForPatientSet_allValidDifferentPatients");
		
		//specs 1-5 are in the last bucket 
		variantSet.add(spec1);  // only 5
		variantSet.add(spec4);  // only 6
		variantSet.add(spec5);  // 5 & 6
		variantSet.add(spec7);  // only #4
		
		patientSet.add(4);
		patientSet.add(5);
		patientSet.add(6);
		
		Collection<String> filteredVariantSet = bucketIndexBySample.filterVariantSetForPatientSet(variantSet, patientSet);
		
		assertEquals("No variants should be filtered out", (long)4, (long)filteredVariantSet.size());
	}
	
	@Test
	public void test_filterVariantSetForPatientSet_someValidDifferentPatients() throws IOException {
		System.out.println("test_filterVariantSetForPatientSet_allValidDifferentPatients");
		
		//specs 1-5 are in the last bucket 
		variantSet.add(spec1); 
		variantSet.add(spec4);  
		variantSet.add(spec5);  
		variantSet.add(spec8);  
		variantSet.add(spec9); //none
		
		patientSet.add(3);
		patientSet.add(9);
		patientSet.add(10);
		
		Collection<String> filteredVariantSet = bucketIndexBySample.filterVariantSetForPatientSet(variantSet, patientSet);
		
		assertEquals("One variant should be filtered out", (long)4, (long)filteredVariantSet.size());
		assertFalse("Spec 9 should have been filtered out", filteredVariantSet.contains(spec9));
	}
	
	@Test
	public void test_filterVariantSetForPatientSet_HeteroPatientA() throws IOException {
		System.out.println("test_filterVariantSetForPatientSet_HeteroPatientA");
		
		variantSet.add(spec8);  //patients 7 and 8 have hetero flags for this variant (1|0 and 0|1)
		
		patientSet.add(7);
		
		Collection<String> filteredVariantSet = bucketIndexBySample.filterVariantSetForPatientSet(variantSet, patientSet);
		
		assertEquals("No variants should be filtered out", (long)1, (long)filteredVariantSet.size());
	}
	
	@Test
	public void test_filterVariantSetForPatientSet_HeteroPatientB() throws IOException {
		System.out.println("test_filterVariantSetForPatientSet_HeteroPatientB");
		
		variantSet.add(spec8);  //patients 7 and 8 have hetero flags for this variant (1|0 and 0|1)
		
		patientSet.add(8);
		
		Collection<String> filteredVariantSet = bucketIndexBySample.filterVariantSetForPatientSet(variantSet, patientSet);
		
		assertEquals("No variants should be filtered out", (long)1, (long)filteredVariantSet.size());
	}
	
	
	@Test
	public void test_filterVariantSetForPatientSet_HeteroNoCallPosPatientA() throws IOException {
		System.out.println("test_filterVariantSetForPatientSet_HeteroNoCallPosPatientA");
		
		variantSet.add(spec8);  //patients 9 and 10 have hetero No Call flags for this variant (1|. and .|1)
		
		patientSet.add(9);
		
		Collection<String> filteredVariantSet = bucketIndexBySample.filterVariantSetForPatientSet(variantSet, patientSet);
		
		assertEquals("No variants should be filtered out", (long)1, (long)filteredVariantSet.size());
	}
	
	@Test
	public void test_filterVariantSetForPatientSet_HeteroNoCallPosPatientB() throws IOException {
		System.out.println("test_filterVariantSetForPatientSet_HeteroNoCallPosPatientB");
		
		variantSet.add(spec8);   //patients 9 and 10 have hetero No Call flags for this variant (1|. and .|1)
		
		patientSet.add(10);
		
		Collection<String> filteredVariantSet = bucketIndexBySample.filterVariantSetForPatientSet(variantSet, patientSet);
		
		assertEquals("No variants should be filtered out", (long)1, (long)filteredVariantSet.size());
	}
	
	@Test
	public void test_filterVariantSetForPatientSet_HeteroNoCallNegPatientA() throws IOException {
		System.out.println("test_filterVariantSetForPatientSet_HeteroNoCallNegPatientA");
		
		variantSet.add(spec10);  //patients 9 and 10 have hetero No Call flags for this variant (0|. and .|0)
		
		patientSet.add(9);
		
		Collection<String> filteredVariantSet = bucketIndexBySample.filterVariantSetForPatientSet(variantSet, patientSet);

		assertTrue("All variants should be filtered out", filteredVariantSet.isEmpty());
	}
	
	@Test
	public void test_filterVariantSetForPatientSet_HeteroNoCallNegPatientB() throws IOException {
		System.out.println("test_filterVariantSetForPatientSet_HeteroNoCallNegPatientB");
		
		variantSet.add(spec10);    //patients 9 and 10 have hetero No Call flags for this variant (0|. and .|0)
		
		patientSet.add(10);
		
		Collection<String> filteredVariantSet = bucketIndexBySample.filterVariantSetForPatientSet(variantSet, patientSet);
		
		assertTrue("All variants should be filtered out", filteredVariantSet.isEmpty());
	}
	
	@Test
	public void test_filterVariantSetForPatientSet_HomoNoCall() throws IOException {
		System.out.println("test_filterVariantSetForPatientSet_HeteroNoCallNegPatientB");
		
		variantSet.add(spec12);   
		patientSet.add(7);
		
		Collection<String> filteredVariantSet = bucketIndexBySample.filterVariantSetForPatientSet(variantSet, patientSet);
		
		assertTrue("All variants should be filtered out", filteredVariantSet.isEmpty());
	}
	
	
	@Test
	public void test_filterVariantSetForPatientSet_HeteroNoCallMultipleVariantsAndPatientsA() throws IOException {
		System.out.println("test_filterVariantSetForPatientSet_HeteroNoCallMultipleVariantsAndPatientss");
		
		variantSet.add(spec10);    //patients 9 and 10 have hetero No Call flags for this variant (0|. and .|0) (#7 has this)
		variantSet.add(spec8);   //patients 9 and 10 have hetero No Call flags for this variant (1|. and .|1)
		variantSet.add(spec4);  // 9 and 10 have a spec in this bucket
		variantSet.add(spec11);  //9 and 10 should not have this spec
		variantSet.add(spec12);   
		
		patientSet.add(1);  //no specs
		patientSet.add(9);
		patientSet.add(10);
		patientSet.add(7);
		
		Collection<String> filteredVariantSet = bucketIndexBySample.filterVariantSetForPatientSet(variantSet, patientSet);
		
		assertEquals("Two variant should be filtered out", (long)3, (long)filteredVariantSet.size());
		assertFalse("Spec 12 should have been filtered out", filteredVariantSet.contains(spec12));
		assertFalse("Spec 11 should have been filtered out", filteredVariantSet.contains(spec11));
	}
	
	
	@Test
	public void test_filterVariantSetForPatientSet_HeteroNoCallMultipleVariantsAndPatientsB() throws IOException {
		System.out.println("test_filterVariantSetForPatientSet_HeteroNoCallMultipleVariantsAndPatientss");
		
		variantSet.add(spec10);    //patients 9 and 10 have hetero No Call flags for this variant (0|. and .|0)
		variantSet.add(spec8);   //patients 9 and 10 have hetero No Call flags for this variant (1|. and .|1)
		variantSet.add(spec4);  // 9 and 10 have a spec in this bucket
		variantSet.add(spec11);  //9 and 10 should not have this spec
		
		patientSet.add(1);  //no specs
		patientSet.add(9);
		patientSet.add(10);
		
		Collection<String> filteredVariantSet = bucketIndexBySample.filterVariantSetForPatientSet(variantSet, patientSet);
		
		assertEquals("Two variant should be filtered out", (long)2, (long)filteredVariantSet.size());
		assertFalse("Spec 10 should have been filtered out", filteredVariantSet.contains(spec10));
		assertFalse("Spec 11 should have been filtered out", filteredVariantSet.contains(spec11));
	}
	
}

package edu.harvard.hms.dbmi.avillach.hpds.etl.genotype;

import edu.harvard.hms.dbmi.avillach.hpds.data.genotype.VariantMasks;
import edu.harvard.hms.dbmi.avillach.hpds.data.genotype.VariantStore;
import edu.harvard.hms.dbmi.avillach.hpds.storage.FileBackedByteIndexedStorage;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public class VariantCounter {

	public static void main(String[] args) throws ClassNotFoundException, IOException {
		VariantStore variantStore = VariantStore.readInstance("/opt/local/hpds/all/");
		for(String contig : variantStore.getVariantMaskStorage().keySet()) {
			int[] countOfVariants = {0};
			FileBackedByteIndexedStorage<Integer, ConcurrentHashMap<String, VariantMasks>>
			currentChromosome = variantStore.getVariantMaskStorage().get(contig);
			currentChromosome.keys().parallelStream().forEach((offsetBucket)->{
				ConcurrentHashMap<String, VariantMasks> maskMap;
				maskMap = currentChromosome.get(offsetBucket);
				if(maskMap!=null) {
					countOfVariants[0]+=maskMap.size();
				}
			});
			System.out.println(contig + "," + countOfVariants[0]);
		}
	}
}

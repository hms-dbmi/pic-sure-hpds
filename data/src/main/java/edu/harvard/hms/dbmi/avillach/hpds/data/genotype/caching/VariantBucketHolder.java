package edu.harvard.hms.dbmi.avillach.hpds.data.genotype.caching;

import java.util.concurrent.ConcurrentHashMap;

public class VariantBucketHolder<K> {
	public ConcurrentHashMap<String, K> lastSetOfVariants;
	public String lastContig;
	public int lastChunkOffset;	
}

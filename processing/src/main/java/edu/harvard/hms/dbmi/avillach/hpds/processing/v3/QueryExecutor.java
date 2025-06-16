package edu.harvard.hms.dbmi.avillach.hpds.processing.v3;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheLoader.InvalidCacheLoadException;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.UncheckedExecutionException;
import edu.harvard.hms.dbmi.avillach.hpds.crypto.Crypto;
import edu.harvard.hms.dbmi.avillach.hpds.data.genotype.FileBackedByteIndexedInfoStore;
import edu.harvard.hms.dbmi.avillach.hpds.data.genotype.InfoColumnMeta;
import edu.harvard.hms.dbmi.avillach.hpds.data.genotype.VariableVariantMasks;
import edu.harvard.hms.dbmi.avillach.hpds.data.genotype.VariantMask;
import edu.harvard.hms.dbmi.avillach.hpds.data.genotype.caching.VariantBucketHolder;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.ColumnMeta;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.PhenoCube;
import edu.harvard.hms.dbmi.avillach.hpds.data.query.v3.*;
import edu.harvard.hms.dbmi.avillach.hpds.processing.DistributableQuery;
import edu.harvard.hms.dbmi.avillach.hpds.processing.GenomicProcessor;
import edu.harvard.hms.dbmi.avillach.hpds.processing.PhenotypeMetaStore;
import edu.harvard.hms.dbmi.avillach.hpds.processing.VariantUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;


@Component
public class QueryExecutor {

	private static Logger log = LoggerFactory.getLogger(QueryExecutor.class);


	private final GenomicProcessor genomicProcessor;

	private final PhenotypicProcessor phenotypicProcessor;


	@Autowired
	public QueryExecutor(
			GenomicProcessor genomicProcessor,
			PhenotypicProcessor phenotypicProcessor
	) throws ClassNotFoundException, IOException, InterruptedException {
		this.genomicProcessor = genomicProcessor;
		this.phenotypicProcessor = phenotypicProcessor;
	}

	public Set<String> getInfoStoreColumns() {
		return genomicProcessor.getInfoStoreColumns();
	}




	/**
	 * Process each filter in the query and return a list of patient ids that should be included in the
	 * result.
	 *
	 * @param query
	 * @return
	 */
	public Set<Integer> getPatientSubsetForQuery(Query query) {
		Set<Integer> patientIdSet = phenotypicProcessor.getPatientSet(query);
		if (patientIdSet == null) {
			return phenotypicProcessor.getPatientIds();
		}
		return patientIdSet;
	}

	public Collection<String> getVariantList(Query query) {
		throw new RuntimeException("Not implemented");
	}

	public List<InfoColumnMeta> getInfoStoreMeta() {
		return genomicProcessor.getInfoColumnMeta();
	}

	public List<String> searchInfoConceptValues(String conceptPath, String query) {
		try {
			return genomicProcessor.getInfoStoreValues(conceptPath).stream()
					.filter(variableValue -> variableValue.toUpperCase(Locale.ENGLISH).contains(query.toUpperCase(Locale.ENGLISH)))
					.collect(Collectors.toList());
		} catch (UncheckedExecutionException e) {
			if(e.getCause() instanceof RuntimeException) {
				throw (RuntimeException) e.getCause();
			}
			throw e;
		}
	}

	//
	//	private boolean pathIsGeneName(String key) {
	//		return new GeneLibrary().geneNameSearch(key).size()==1;
	//	}



	public TreeMap<String, ColumnMeta> getDictionary() {
		return phenotypicProcessor.getMetaStore();
	}

	public List<String> getPatientIds() {
		return genomicProcessor.getPatientIds();
	}

	public Optional<VariableVariantMasks> getMasks(String path, VariantBucketHolder<VariableVariantMasks> variantMasksVariantBucketHolder) {
		return genomicProcessor.getMasks(path, variantMasksVariantBucketHolder);
	}

    // todo: handle this locally, we do not want this in the genomic processor
    protected VariantMask createMaskForPatientSet(Set<Integer> patientSubset) {
        return genomicProcessor.createMaskForPatientSet(patientSubset);
    }
}

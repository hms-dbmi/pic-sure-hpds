package edu.harvard.hms.dbmi.avillach.hpds.processing;

import com.google.common.collect.Sets;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class SparseVariantIndex extends VariantIndex {

    private final Set<Integer> variantIds;

    public SparseVariantIndex(Set<Integer> variantIds) {
        this.variantIds = variantIds;
    }

    public Set<Integer> getVariantIds() {
        return variantIds;
    }

    @Override
    public VariantIndex union(VariantIndex variantIndex) {
        if (variantIndex instanceof SparseVariantIndex) {
            return new SparseVariantIndex(Sets.union(((SparseVariantIndex) variantIndex).variantIds, variantIds));
        } else if (variantIndex instanceof DenseVariantIndex) {
            return union(this, (DenseVariantIndex) variantIndex);
        } else {
            throw new IllegalArgumentException("Union not implemented between SparseVariantIndex and " + variantIndex.getClass());
        }
    }

    @Override
    public VariantIndex intersection(VariantIndex variantIndex) {
        if (variantIndex instanceof SparseVariantIndex) {
            return new SparseVariantIndex(Sets.intersection(((SparseVariantIndex) variantIndex).variantIds, variantIds));
        } else if (variantIndex instanceof DenseVariantIndex) {
            return intersection(this, (DenseVariantIndex) variantIndex);
        } else {
            throw new IllegalArgumentException("Intersection not implemented between SparseVariantIndex and " + variantIndex.getClass());
        }
    }

    @Override
    public Set<String> mapToVariantSpec(String[] variantIndex) {
        ConcurrentHashMap<String, String> setMap = new ConcurrentHashMap<>(variantIds.size());
        variantIds.stream().parallel().forEach(index-> setMap.put(variantIndex[index], ""));
        return setMap.keySet();
    }

    @Override
    public boolean isEmpty() {
        return variantIds.isEmpty();
    }
}

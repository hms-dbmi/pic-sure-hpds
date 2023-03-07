package edu.harvard.hms.dbmi.avillach.hpds.processing;

import com.google.common.collect.Sets;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class DenseVariantIndex extends VariantIndex {

    private final boolean[] variantIndexMask;

    public DenseVariantIndex(boolean[] variantIndexMask) {
        this.variantIndexMask = variantIndexMask;
    }

    public boolean[] getVariantIndexMask() {
        return variantIndexMask;
    }

    @Override
    protected VariantIndex union(VariantIndex variantIndex) {
        if (variantIndex instanceof SparseVariantIndex) {
            return union((SparseVariantIndex) variantIndex, this);
        } else if (variantIndex instanceof DenseVariantIndex) {
            // todo: implement with arrays of different lengths
            boolean[] copy = new boolean[variantIndexMask.length];
            for (int i = 0; i < copy.length; i++) {
                copy[i] = variantIndexMask[i] || ((DenseVariantIndex) variantIndex).variantIndexMask[i];
            }
            return new DenseVariantIndex(copy);
        } else {
            throw new IllegalArgumentException("Union not implemented between DenseVariantIndex and " + variantIndex.getClass());
        }
    }

    @Override
    protected VariantIndex intersection(VariantIndex variantIndex) {
        if (variantIndex instanceof SparseVariantIndex) {
            return intersection((SparseVariantIndex) variantIndex, this);
        } else if (variantIndex instanceof DenseVariantIndex) {
            // todo: implement with arrays of different lengths
            boolean[] copy = new boolean[variantIndexMask.length];
            for (int i = 0; i < copy.length; i++) {
                copy[i] = variantIndexMask[i] && ((DenseVariantIndex) variantIndex).variantIndexMask[i];
            }
            // todo: return sparse index if small
            return new DenseVariantIndex(copy);
        } else {
            throw new IllegalArgumentException("Intersection not implemented between SparseVariantIndex and " + variantIndex.getClass());
        }
    }

    @Override
    protected Set<String> mapToVariantSpec(String[] variantIndex) {
        ConcurrentHashMap<String, String> setMap = new ConcurrentHashMap<>(variantIndexMask.length / 10);
        for (int i = 0; i < variantIndexMask.length; i++) {
            if (variantIndexMask[i])
                setMap.put(variantIndex[i], "");
        }
        return setMap.keySet();
    }
}

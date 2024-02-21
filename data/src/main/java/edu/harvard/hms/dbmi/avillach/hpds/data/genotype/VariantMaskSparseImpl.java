package edu.harvard.hms.dbmi.avillach.hpds.data.genotype;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashSet;
import java.util.Set;

public class VariantMaskSparseImpl implements VariantMask {

    @JsonProperty("p")
    protected Set<Integer> patientIndexes;

    public VariantMaskSparseImpl(@JsonProperty("p") Set<Integer> patientIndexes) {
        this.patientIndexes = patientIndexes;
    }

    public Set<Integer> getPatientIndexes() {
        return patientIndexes;
    }

    @Override
    public VariantMask intersection(VariantMask variantMask) {
        throw new RuntimeException("Not implemented yet");
    }

    @Override
    public VariantMask union(VariantMask variantMask) {
        if (variantMask instanceof VariantMaskBitmaskImpl) {
            return VariantMask.union(this, (VariantMaskBitmaskImpl) variantMask);
        } else if (variantMask instanceof  VariantMaskSparseImpl) {
            return union((VariantMaskSparseImpl) variantMask);
        } else {
            throw new RuntimeException("Unknown VariantMask implementation");
        }
    }

    @Override
    public boolean testBit(int bit) {
        return patientIndexes.contains(bit);
    }

    @Override
    public int bitCount() {
        return patientIndexes.size();
    }

    private VariantMask union(VariantMaskSparseImpl variantMask) {
        HashSet<Integer> union = new HashSet<>(variantMask.patientIndexes);
        union.addAll(this.patientIndexes);
        return new VariantMaskSparseImpl(union);
    }
}

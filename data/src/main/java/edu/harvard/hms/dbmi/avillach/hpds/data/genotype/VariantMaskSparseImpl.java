package edu.harvard.hms.dbmi.avillach.hpds.data.genotype;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigInteger;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class VariantMaskSparseImpl implements VariantMask {

    @JsonProperty("ids")
    protected Set<Integer> patientIds;

    @JsonProperty("size")
    private int size;

    public VariantMaskSparseImpl(@JsonProperty("ids") Set<Integer> patientIds, @JsonProperty("size") int size) {
        this.patientIds = patientIds;
        this.size = size;
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

    private VariantMask union(VariantMaskSparseImpl variantMask) {
        HashSet<Integer> union = new HashSet<>(variantMask.patientIds);
        union.addAll(this.patientIds);
        return new VariantMaskSparseImpl(union, Math.max(variantMask.size, this.size));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VariantMaskSparseImpl that = (VariantMaskSparseImpl) o;
        return size == that.size && Objects.equals(patientIds, that.patientIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(patientIds, size);
    }
}

package edu.harvard.hms.dbmi.avillach.hpds.data.genotype;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;

import java.math.BigInteger;
import java.util.Objects;

public class VariantMaskBitmaskImpl implements VariantMask {


    @JsonProperty("mask")
    @JsonSerialize(using = ToStringSerializer.class)
    protected final BigInteger bitmask;

    public BigInteger getBitmask() {
        return bitmask;
    }

    @JsonCreator
    public VariantMaskBitmaskImpl(@JsonProperty("mask") BigInteger bitmask) {
        this.bitmask = bitmask;
    }

    @Override
    public VariantMask intersection(VariantMask variantMask) {
        throw new RuntimeException("Not implemented yet");
    }

    @Override
    public VariantMask union(VariantMask variantMask) {
        if (variantMask instanceof VariantMaskBitmaskImpl) {
            return union((VariantMaskBitmaskImpl) variantMask);
        } else if (variantMask instanceof  VariantMaskSparseImpl) {
            return VariantMask.union((VariantMaskSparseImpl) variantMask, this);
        } else {
            throw new RuntimeException("Unknown VariantMask implementation");
        }
    }

    private VariantMask union(VariantMaskBitmaskImpl variantMaskBitmask) {
        return new VariantMaskBitmaskImpl(variantMaskBitmask.bitmask.and(this.bitmask));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VariantMaskBitmaskImpl that = (VariantMaskBitmaskImpl) o;
        return Objects.equals(bitmask, that.bitmask);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bitmask);
    }
}

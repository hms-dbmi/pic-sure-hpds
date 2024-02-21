package edu.harvard.hms.dbmi.avillach.hpds.data.genotype;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.math.BigInteger;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type",
        defaultImpl = VariantMaskBitmaskImpl.class
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = VariantMaskBitmaskImpl.class, name = "b"),
        @JsonSubTypes.Type(value = VariantMaskSparseImpl.class, name = "s")
})
public interface VariantMask {

    VariantMask intersection(VariantMask variantMask);

    VariantMask union(VariantMask variantMask);

    static VariantMask union(VariantMaskSparseImpl variantMaskSparse, VariantMaskBitmaskImpl variantMaskBitmask) {
        BigInteger union = variantMaskBitmask.bitmask;
        for (Integer patientId : variantMaskSparse.patientIndexes) {
            union = union.setBit(patientId + 2);
        }
        return new VariantMaskBitmaskImpl(union);
    }
}

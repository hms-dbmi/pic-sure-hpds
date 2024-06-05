package edu.harvard.hms.dbmi.avillach.hpds.processing;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class VariantUtilsTest {

    @Test
    public void pathIsVariantSpec_oldFormat() {
        assertTrue(VariantUtils.pathIsVariantSpec("chr21,5032061,A,G"));
    }
    @Test
    public void pathIsVariantSpec_newFormat() {
        assertTrue(VariantUtils.pathIsVariantSpec("chr21,5032061,A,G,LOC102723996,missense_variant"));
    }
}
package edu.harvard.hms.dbmi.avillach.hpds.etl.genotype;

public class VCFIndexLine implements Comparable<VCFIndexLine> {
    String vcfPath;
    String contig;
    boolean isAnnotated;
    boolean isGzipped;
    String[] sampleIds;
    Integer[] patientIds;

    @Override
    public int compareTo(VCFIndexLine o) {
        int chomosomeComparison = o.contig == null ? 1 : contig == null ? 0 : contig.compareTo(o.contig);
        if (chomosomeComparison == 0) {
            return vcfPath.compareTo(o.vcfPath);
        }
        return chomosomeComparison;
    }
}

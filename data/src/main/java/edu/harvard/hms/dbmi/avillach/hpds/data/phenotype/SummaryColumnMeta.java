package edu.harvard.hms.dbmi.avillach.hpds.data.phenotype;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SummaryColumnMeta {

    private static final Logger log = LoggerFactory.getLogger(SummaryColumnMeta.class);

    private String name;
    private int widthInBytes;
    private boolean categorical;
    private List<String> categoryValues = List.of();
    private Double min, max;
    private int patientCount;
    private boolean hasTimestamp;
    private Long timestampMin;
    private Long timestampMax;

    public SummaryColumnMeta(ColumnMeta columnMeta) {
        this.name = columnMeta.getName();
        this.widthInBytes = columnMeta.getWidthInBytes();
        this.categorical = columnMeta.isCategorical();
        this.categoryValues = columnMeta.getCategoryValues();
        this.min = columnMeta.getMin();
        this.max = columnMeta.getMax();
        this.patientCount = columnMeta.getPatientCount();
        this.hasTimestamp = columnMeta.hasTimestamp();
        this.timestampMin = columnMeta.getTimestampMin();
        this.timestampMax = columnMeta.getTimestampMax();
    }

    public SummaryColumnMeta() {}

    public SummaryColumnMeta merge(ColumnMeta columnMeta) {
        SummaryColumnMeta newSummaryColumnMeta = new SummaryColumnMeta();

        if (!Objects.equals(this.name, columnMeta.getName())) {
            log.warn("ColumnMeta names do not match: {} and {}", this.name, columnMeta.getName());
        }
        newSummaryColumnMeta.name = this.name;

        if (this.widthInBytes != columnMeta.getWidthInBytes()) {
            log.warn(
                "ColumnMeta {} widthInBytes values do not match: {} and {}", this.name, this.widthInBytes, columnMeta.getWidthInBytes()
            );
        }
        newSummaryColumnMeta.widthInBytes = Integer.max(this.widthInBytes, columnMeta.getWidthInBytes());

        if (this.categorical != columnMeta.isCategorical()) {
            log.warn("ColumnMeta {} categorical values do not match: {} and {}", this.name, this.categorical, columnMeta.isCategorical());
        }
        newSummaryColumnMeta.categorical = this.categorical;

        newSummaryColumnMeta.categoryValues = Stream.concat(
            categoryValues != null ? categoryValues.stream() : Stream.of(),
            columnMeta.getCategoryValues() != null ? columnMeta.getCategoryValues().stream() : Stream.of()
        ).collect(Collectors.toList());

        if (columnMeta.getMin() != null) {
            newSummaryColumnMeta.min = this.min == null ? columnMeta.getMin() : Double.min(this.min, columnMeta.getMin());
        } else {
            newSummaryColumnMeta.min = this.min;
        }

        if (columnMeta.getMax() != null) {
            newSummaryColumnMeta.max = this.max == null ? columnMeta.getMax() : Double.max(this.max, columnMeta.getMax());
        } else {
            newSummaryColumnMeta.max = this.max;
        }

        // todo: can patients be in different partitions for the same concept path? if so, this will be inaccurate
        newSummaryColumnMeta.patientCount = this.patientCount + columnMeta.getPatientCount();
        newSummaryColumnMeta.hasTimestamp = this.hasTimestamp || columnMeta.hasTimestamp();

        if (columnMeta.getTimestampMin() != null) {
            newSummaryColumnMeta.timestampMin =
                this.timestampMin == null ? columnMeta.getTimestampMin() : Long.min(this.timestampMin, columnMeta.getTimestampMin());
        } else {
            newSummaryColumnMeta.timestampMin = this.timestampMin;
        }

        if (columnMeta.getTimestampMax() != null) {
            newSummaryColumnMeta.timestampMax =
                this.timestampMax == null ? columnMeta.getTimestampMax() : Long.max(this.timestampMax, columnMeta.getTimestampMax());
        } else {
            newSummaryColumnMeta.timestampMax = this.timestampMax;
        }

        return newSummaryColumnMeta;
    }


    public String getName() {
        return name;
    }

    public int getWidthInBytes() {
        return widthInBytes;
    }

    public boolean isCategorical() {
        return categorical;
    }

    public List<String> getCategoryValues() {
        return categoryValues;
    }

    public Double getMin() {
        return min;
    }

    public Double getMax() {
        return max;
    }

    public int getPatientCount() {
        return patientCount;
    }

    public boolean isHasTimestamp() {
        return hasTimestamp;
    }

    public Long getTimestampMin() {
        return timestampMin;
    }

    public Long getTimestampMax() {
        return timestampMax;
    }

    public SummaryColumnMeta setName(String name) {
        this.name = name;
        return this;
    }

    public SummaryColumnMeta setCategorical(boolean categorical) {
        this.categorical = categorical;
        return this;
    }

    public SummaryColumnMeta setCategoryValues(List<String> categoryValues) {
        this.categoryValues = categoryValues;
        return this;
    }

    public SummaryColumnMeta setMin(double min) {
        this.min = min;
        return this;
    }

    public SummaryColumnMeta setMax(double max) {
        this.max = max;
        return this;
    }

    public SummaryColumnMeta setPatientCount(int patientCount) {
        this.patientCount = patientCount;
        return this;
    }

    public SummaryColumnMeta setHasTimestamp(boolean hasTimestamp) {
        this.hasTimestamp = hasTimestamp;
        return this;
    }

    public SummaryColumnMeta setTimestampMin(Long timestampMin) {
        this.timestampMin = timestampMin;
        return this;
    }

    public SummaryColumnMeta setTimestampMax(Long timestampMax) {
        this.timestampMax = timestampMax;
        return this;
    }
}

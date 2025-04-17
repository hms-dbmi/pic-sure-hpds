package edu.harvard.hms.dbmi.avillach.hpds.etl.phenotype.config;

public class CSVConfig {
    private String dataset_name;
    private boolean dataset_name_as_root_node;

    public String getDataset_name() {
        return dataset_name;
    }

    public void setDataset_name(String dataset_name) {
        this.dataset_name = dataset_name;
    }

    public boolean isDataset_name_as_root_node() {
        return dataset_name_as_root_node;
    }

    public void setDataset_name_as_root_node(boolean dataset_name_as_root_node) {
        this.dataset_name_as_root_node = dataset_name_as_root_node;
    }
}

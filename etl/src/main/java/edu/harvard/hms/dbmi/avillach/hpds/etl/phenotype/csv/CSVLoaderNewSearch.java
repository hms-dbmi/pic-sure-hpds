package edu.harvard.hms.dbmi.avillach.hpds.etl.phenotype.csv;

import edu.harvard.hms.dbmi.avillach.hpds.crypto.Crypto;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.PhenoCube;
import edu.harvard.hms.dbmi.avillach.hpds.etl.LoadingStore;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.Date;

@SpringBootApplication
@ComponentScan(
        basePackages = "edu.harvard.hms.dbmi.avillach.hpds",
        includeFilters = @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, classes = Crypto.class)
)
public class CSVLoaderNewSearch {

    private static final Logger log = LoggerFactory.getLogger(CSVLoaderNewSearch.class);

    public static void main(String[] args) {
        SpringApplication.run(CSVLoaderNewSearch.class, args);
    }

    @Bean
    ApplicationRunner runCSVLoader(CSVLoaderService csvLoaderService) {
        return args -> csvLoaderService.runEtlProcess();
    }
}

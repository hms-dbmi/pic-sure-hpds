package edu.harvard.hms.dbmi.avillach.hpds.etl.phenotype.csv;

import edu.harvard.hms.dbmi.avillach.hpds.crypto.Crypto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;

@SpringBootApplication
@ComponentScan(
        basePackages = "edu.harvard.hms.dbmi.avillach.hpds",
        includeFilters = @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, classes = Crypto.class)
)
public class Loader {

    private static final Logger log = LoggerFactory.getLogger(Loader.class);

    public static void main(String[] args) {
        SpringApplication.run(Loader.class, args);
    }

    @Bean
    ApplicationRunner runCSVLoader(LoaderService loaderService) {
        return args -> loaderService.runEtlProcess();
    }

}

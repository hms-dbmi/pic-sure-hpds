package edu.harvard.hms.dbmi.avillach.hpds.utilities.compare;

import edu.harvard.hms.dbmi.avillach.hpds.utilities.compare.config.CompareConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Spring Boot application for comparing two columnMeta.javabin files.
 *
 * <p>This is a standalone utility application separate from IngestServiceApplication.
 * It generates detailed comparison reports including:
 * <ul>
 *   <li>Matching keys and missing keys (in both directions)</li>
 *   <li>Metadata changes (type, counts, categories, min/max)</li>
 *   <li>8 detailed CSV/TXT reports</li>
 * </ul>
 *
 * <p>Usage:
 * <pre>
 * java -cp ingest-service.jar \
 *   edu.harvard.hms.dbmi.avillach.hpds.utilities.compare.CompareColumnMetaApplication \
 *   --compare.file-a=/path/to/A/columnMeta.javabin \
 *   --compare.file-b=/path/to/B/columnMeta.javabin \
 *   --compare.output-dir=/path/to/output
 * </pre>
 *
 * <p>Or use the wrapper script:
 * <pre>
 * ./bin/compare-columnmeta.sh \
 *   /path/to/A/columnMeta.javabin \
 *   /path/to/B/columnMeta.javabin \
 *   /path/to/output
 * </pre>
 */
@SpringBootApplication(
    scanBasePackages = "edu.harvard.hms.dbmi.avillach.hpds.utilities.compare",
    exclude = {
        org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration.class,
        org.springframework.boot.autoconfigure.batch.BatchAutoConfiguration.class
    }
)
public class CompareColumnMetaApplication implements CommandLineRunner {
    private static final Logger log = LoggerFactory.getLogger(CompareColumnMetaApplication.class);

    private final CompareConfig config;
    private final CompareColumnMetaUtility utility;

    public CompareColumnMetaApplication(CompareConfig config, CompareColumnMetaUtility utility) {
        this.config = config;
        this.utility = utility;
    }

    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(CompareColumnMetaApplication.class);
        app.setWebApplicationType(org.springframework.boot.WebApplicationType.NONE);
        app.run(args);
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("Starting ColumnMeta comparison");
        utility.compare();
        log.info("Comparison complete");
    }
}

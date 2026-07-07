package edu.harvard.hms.dbmi.avillach.hpds.service.health;

import edu.harvard.hms.dbmi.avillach.hpds.processing.AbstractProcessor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Deep readiness for HPDS: a real "data ready" signal rather than just port-up. HPDS has no DataSource, so this replaces the built-in db
 * indicator.
 *
 * <p>Genomic data is optional — open-access and phenotype-only deployments run without it. When genomic support is not configured
 * ({@code hpds.genomicProcessor.impl} unset), readiness depends on phenotype data alone and the genomic store is not inspected. When it is
 * configured, either phenotype or genomic data being loaded is enough to report UP.
 */
@Component("hpdsReadiness")
public class HpdsReadinessHealthIndicator implements HealthIndicator {

    private final AbstractProcessor abstractProcessor;
    private final boolean genomicEnabled;

    public HpdsReadinessHealthIndicator(
        AbstractProcessor abstractProcessor, @Value("${hpds.genomicProcessor.impl:}") String genomicProcessorImpl
    ) {
        this.abstractProcessor = abstractProcessor;
        this.genomicEnabled = genomicProcessorImpl != null && !genomicProcessorImpl.isBlank();
    }

    @Override
    public Health health() {
        try {
            int phenotypeColumns = abstractProcessor.getDictionary().size();
            Health.Builder builder = Health.unknown().withDetail("phenotypeColumns", phenotypeColumns);

            boolean dataLoaded = phenotypeColumns > 0;
            if (genomicEnabled) {
                int genomicColumns = abstractProcessor.getInfoStoreColumns().size();
                builder.withDetail("genomicColumns", genomicColumns);
                dataLoaded = dataLoaded || genomicColumns > 0;
            } else {
                builder.withDetail("genomicData", "not configured");
            }

            String reason = genomicEnabled ? "no phenotype or genomic data loaded" : "no phenotype data loaded";
            return (dataLoaded ? builder.up() : builder.down().withDetail("reason", reason)).build();
        } catch (Exception ex) {
            return Health.down(ex).build();
        }
    }
}

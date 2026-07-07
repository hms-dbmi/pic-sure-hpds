package edu.harvard.hms.dbmi.avillach.hpds.service.health;

import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.ColumnMeta;
import edu.harvard.hms.dbmi.avillach.hpds.processing.AbstractProcessor;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import java.util.Set;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class HpdsReadinessHealthIndicatorTest {

    private static final String GENOMIC_IMPL = "localDistributed";

    private static AbstractProcessor processorWith(TreeMap<String, ColumnMeta> dictionary, Set<String> infoStoreColumns) {
        AbstractProcessor proc = mock(AbstractProcessor.class);
        when(proc.getDictionary()).thenReturn(dictionary);
        when(proc.getInfoStoreColumns()).thenReturn(infoStoreColumns);
        return proc;
    }

    private static TreeMap<String, ColumnMeta> dictionaryWithOneColumn() {
        TreeMap<String, ColumnMeta> dict = new TreeMap<>();
        dict.put("\\demographics\\AGE\\", mock(ColumnMeta.class));
        return dict;
    }

    @Test
    void upWhenPhenotypeDataLoaded() {
        AbstractProcessor proc = processorWith(dictionaryWithOneColumn(), Set.of());

        assertThat(new HpdsReadinessHealthIndicator(proc, GENOMIC_IMPL).health().getStatus()).isEqualTo(Status.UP);
    }

    @Test
    void upWhenGenomicDataLoaded() {
        AbstractProcessor proc = processorWith(new TreeMap<>(), Set.of("Gene_with_variant"));

        assertThat(new HpdsReadinessHealthIndicator(proc, GENOMIC_IMPL).health().getStatus()).isEqualTo(Status.UP);
    }

    @Test
    void downWhenGenomicEnabledButNoDataLoaded() {
        AbstractProcessor proc = processorWith(new TreeMap<>(), Set.of());

        assertThat(new HpdsReadinessHealthIndicator(proc, GENOMIC_IMPL).health().getStatus()).isEqualTo(Status.DOWN);
    }

    @Test
    void upWhenGenomicDisabledAndPhenotypeLoaded() {
        AbstractProcessor proc = mock(AbstractProcessor.class);
        when(proc.getDictionary()).thenReturn(dictionaryWithOneColumn());

        Health health = new HpdsReadinessHealthIndicator(proc, "").health();

        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(health.getDetails()).containsEntry("genomicData", "not configured");
        // Genomic store must not be inspected when genomic support is not configured.
        verify(proc, never()).getInfoStoreColumns();
    }

    @Test
    void downWhenGenomicDisabledAndNoPhenotypeLoaded() {
        AbstractProcessor proc = mock(AbstractProcessor.class);
        when(proc.getDictionary()).thenReturn(new TreeMap<>());

        assertThat(new HpdsReadinessHealthIndicator(proc, "").health().getStatus()).isEqualTo(Status.DOWN);
        verify(proc, never()).getInfoStoreColumns();
    }
}

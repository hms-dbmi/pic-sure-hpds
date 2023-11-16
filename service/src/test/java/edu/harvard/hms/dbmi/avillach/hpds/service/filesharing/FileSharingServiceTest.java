package edu.harvard.hms.dbmi.avillach.hpds.service.filesharing;

import edu.harvard.hms.dbmi.avillach.hpds.data.query.Query;
import edu.harvard.hms.dbmi.avillach.hpds.processing.AsyncResult;
import edu.harvard.hms.dbmi.avillach.hpds.processing.VariantListProcessor;
import edu.harvard.hms.dbmi.avillach.hpds.service.QueryService;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;

@RunWith(MockitoJUnitRunner.class)
public class FileSharingServiceTest {

    @Mock
    QueryService queryService;

    @Mock
    FileSystemService fileWriter;

    @Mock
    VariantListProcessor variantListProcessor;

    @InjectMocks
    FileSharingService subject;

    @Test
    public void shouldCreatePhenotypicData() {
        Query query = new Query();
        query.setId("my-id");
        AsyncResult result = new AsyncResult(query, new String[]{});
        result.status = AsyncResult.Status.SUCCESS;

        Mockito.when(queryService.getResultFor("my-id"))
            .thenReturn(result);
        Mockito.when(fileWriter.writeResultToFile("phenotypic_data.tsv", "my-id", result))
            .thenReturn(true);

        boolean actual = subject.createPhenotypicData(query);

        Assert.assertTrue(actual);
    }

    @Test
    public void shouldNotCreatePhenotypicData() {
        Query query = new Query();
        query.setId("my-id");
        AsyncResult result = new AsyncResult(query, new String[]{});
        result.status = AsyncResult.Status.ERROR;

        Mockito.when(queryService.getResultFor("my-id"))
            .thenReturn(result);

        boolean actual = subject.createPhenotypicData(query);

        Assert.assertFalse(actual);
    }

    @Test
    public void shouldCreateGenomicData() throws IOException {
        Query query = new Query();
        query.setId("my-id");
        String vcf = "lol lets put the whole vcf in a string";
        Mockito.when(variantListProcessor.runVcfExcerptQuery(query, true))
            .thenReturn(vcf);
        Mockito.when(fileWriter.writeResultToFile("genomic_data.tsv", "my-id", vcf, "my-id"))
            .thenReturn(true);

        boolean actual = subject.createGenomicData(query);

        Assert.assertTrue(actual);
    }

    @Test
    public void shouldNotCreateGenomicData() throws IOException {
        Query query = new Query();
        query.setId("my-id");
        Mockito.when(variantListProcessor.runVcfExcerptQuery(query, true))
            .thenThrow(new IOException("oh no!"));

        boolean actual = subject.createGenomicData(query);

        Assert.assertFalse(actual);
    }
}
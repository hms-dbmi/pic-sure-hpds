package edu.harvard.hms.dbmi.avillach.hpds.processing.timeseries;

import org.junit.Test;

import java.util.Optional;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;


public class TimeSeriesConversionServiceTest {

    TimeSeriesConversionService subject = new TimeSeriesConversionService();

    @Test
    public void shouldConvertToIsoString() {
        String actual = subject.toISOString(0L).get();
        String expected = "1970-01-01T00:00:00Z";

        assertEquals(expected, actual);
    }

    @Test
    public void shouldEmptyForNull() {
        Optional<String> actual = subject.toISOString(null);
        Optional<String> expected = Optional.empty();

        assertEquals(expected, actual);
    }
}
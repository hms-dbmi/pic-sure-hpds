package edu.harvard.hms.dbmi.avillach.hpds.processing.timeseries;

import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

@Service
public class TimeSeriesConversionService {

    public Optional<String> toISOString(Long unixTimeStamp) {
        if (unixTimeStamp == null) {
            return Optional.empty();
        }
        Instant instant = Instant.ofEpochMilli(unixTimeStamp);
        return Optional.of(DateTimeFormatter.ISO_INSTANT.format(instant));
    }
}

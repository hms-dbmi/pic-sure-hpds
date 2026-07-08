package edu.harvard.hms.dbmi.avillach.hpds.ingest.producer;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.format.DateTimeParseException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for TimestampParser - validates the fallback chain shared by CSV and Parquet producers.
 */
class TimestampParserTest {

    @Test
    void parsesIso8601WithZone() {
        assertEquals(Instant.parse("2021-01-15T10:30:00Z"), TimestampParser.parse("2021-01-15T10:30:00Z"));
        assertEquals(Instant.parse("2021-01-15T10:30:00Z"), TimestampParser.parse("2021-01-15T10:30:00+00:00"));
    }

    @Test
    void parsesSpaceSeparatedAsUtc() {
        // EHR export format: space-separated, zoneless, treated as UTC
        assertEquals(Instant.parse("2023-11-25T14:00:00Z"), TimestampParser.parse("2023-11-25 14:00:00"));
        assertEquals(Instant.parse("2024-06-27T00:00:00Z"), TimestampParser.parse("2024-06-27 00:00:00"));
    }

    @Test
    void parsesSpaceSeparatedWithFractionalSecondsAsUtc() {
        // EHR exports mix fractional-second precision on the same column
        assertEquals(Instant.parse("2020-01-02T03:04:05.678Z"), TimestampParser.parse("2020-01-02 03:04:05.678"));
        assertEquals(Instant.parse("2021-06-07T08:09:10.900Z"), TimestampParser.parse("2021-06-07 08:09:10.9"));
        assertEquals(Instant.parse("2022-03-04T05:06:07.030Z"), TimestampParser.parse("2022-03-04 05:06:07.03"));
    }

    @Test
    void parsesIsoLocalDateTimeAsUtc() {
        assertEquals(Instant.parse("2025-02-28T12:56:40.500Z"), TimestampParser.parse("2025-02-28T12:56:40.500"));
    }

    @Test
    void parsesIsoLocalDateAsMidnightUtc() {
        assertEquals(Instant.parse("2025-02-28T00:00:00Z"), TimestampParser.parse("2025-02-28"));
    }

    @Test
    void throwsOnGarbage() {
        assertThrows(DateTimeParseException.class, () -> TimestampParser.parse("invalid-timestamp"));
        assertThrows(DateTimeParseException.class, () -> TimestampParser.parse("0"));
    }

    @Test
    void throwsOnOutOfRangeFields() {
        assertThrows(DateTimeParseException.class, () -> TimestampParser.parse("2023-13-45 99:00:00"));
    }
}

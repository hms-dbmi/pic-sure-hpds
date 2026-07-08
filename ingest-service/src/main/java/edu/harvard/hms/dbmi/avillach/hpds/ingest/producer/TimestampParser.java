package edu.harvard.hms.dbmi.avillach.hpds.ingest.producer;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;

/**
 * Shared timestamp parsing for observation producers.
 *
 * <p>Deterministic fallback chain (zoneless formats are treated as UTC): <ol> <li>ISO-8601 instant with zone/offset (e.g.,
 * {@code 2021-01-15T10:30:00Z})</li> <li>Space-separated local date-time (e.g., {@code 2023-11-25 14:00:00}) - EHR export format</li>
 * <li>ISO-8601 local date-time (e.g., {@code 2025-02-28T12:56:40.500})</li> <li>ISO-8601 local date (e.g., {@code 2025-02-28}) - midnight
 * UTC</li> </ol>
 *
 * <p>Null-like guards ("0", "None", blank, epoch numbers) are the callers' responsibility; this parser throws on anything it cannot parse
 * so callers can distinguish invalid from absent.
 */
final class TimestampParser {

    // "uuuu-MM-dd HH:mm:ss" with optional fractional seconds of any precision
    // (real EHR exports mix "2023-11-25 14:00:00", "...53.547", "...15.9")
    private static final DateTimeFormatter SPACE_SEPARATED =
        new DateTimeFormatterBuilder().appendPattern("uuuu-MM-dd HH:mm:ss").optionalStart()
            .appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true).optionalEnd().toFormatter().withResolverStyle(ResolverStyle.STRICT);

    private TimestampParser() {}

    /**
     * Parses a raw timestamp string into an Instant.
     *
     * @param raw non-null, non-blank timestamp string
     * @return parsed Instant
     * @throws DateTimeParseException if no supported format matches
     */
    static Instant parse(String raw) {
        try {
            // Attempt 1: ISO-8601 instant (requires Z or offset)
            return Instant.parse(raw);
        } catch (DateTimeParseException e1) {
            try {
                // Attempt 2: space-separated local date-time (EHR format), treat as UTC
                return LocalDateTime.parse(raw, SPACE_SEPARATED).toInstant(ZoneOffset.UTC);
            } catch (DateTimeParseException e2) {
                try {
                    // Attempt 3: ISO-8601 local date-time, treat as UTC
                    return LocalDateTime.parse(raw, DateTimeFormatter.ISO_LOCAL_DATE_TIME).toInstant(ZoneOffset.UTC);
                } catch (DateTimeParseException e3) {
                    // Attempt 4: ISO-8601 local date, treat as midnight UTC; throws if unparseable
                    return LocalDate.parse(raw, DateTimeFormatter.ISO_LOCAL_DATE).atStartOfDay().toInstant(ZoneOffset.UTC);
                }
            }
        }
    }
}

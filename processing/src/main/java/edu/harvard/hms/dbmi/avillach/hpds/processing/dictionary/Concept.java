package edu.harvard.hms.dbmi.avillach.hpds.processing.dictionary;

import java.util.Map;

public record Concept(String conceptPath, String name, Map<String, String> meta) {
}

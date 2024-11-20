package edu.harvard.hms.dbmi.avillach.hpds.processing.dictionary;

import java.util.Map;

public record Concept(String type, String conceptPath, String name, String display, String dataset, String description, Map<String, String> meta) {
}

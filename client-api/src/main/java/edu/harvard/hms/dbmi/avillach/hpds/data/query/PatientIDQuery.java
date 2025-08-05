package edu.harvard.hms.dbmi.avillach.hpds.data.query;

import java.util.Set;

public record PatientIDQuery(InclusionRule rule, Set<Integer> patients) {
}

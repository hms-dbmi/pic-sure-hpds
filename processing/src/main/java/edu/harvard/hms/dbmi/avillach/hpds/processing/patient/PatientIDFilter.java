package edu.harvard.hms.dbmi.avillach.hpds.processing.patient;

import edu.harvard.hms.dbmi.avillach.hpds.data.query.PatientIDQuery;
import edu.harvard.hms.dbmi.avillach.hpds.data.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.Set;

@Service
@ConditionalOnProperty(value = "hpds.enablePatientFilters", havingValue = "true")
public class PatientIDFilter {

    private static final Logger log = LoggerFactory.getLogger(PatientIDFilter.class);

    public Set<Integer> applyPatientFilter(Query query, Set<Integer> patientIDs) {
        PatientIDQuery idQuery = query.getPatientIDQuery();
        if (idQuery == null || idQuery.patients() == null || idQuery.rule() == null || idQuery.patients().isEmpty()) {
            return patientIDs;
        }
        Set<Integer> mutableIDs = new HashSet<>(patientIDs);
        log.info("{} {} patient IDs from query {}", idQuery.rule(), idQuery.patients().size(), query.getPicSureId());
        switch (idQuery.rule()) {
            case Exclude -> mutableIDs.removeAll(idQuery.patients());
            case Include -> mutableIDs.addAll(idQuery.patients());
            case Only -> mutableIDs = idQuery.patients();
        }
        return mutableIDs;
    }
}

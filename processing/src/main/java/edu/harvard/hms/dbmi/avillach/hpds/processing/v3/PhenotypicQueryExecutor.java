package edu.harvard.hms.dbmi.avillach.hpds.processing.v3;

import com.google.common.collect.Sets;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.ColumnMeta;
import edu.harvard.hms.dbmi.avillach.hpds.data.query.v3.*;
import edu.harvard.hms.dbmi.avillach.hpds.processing.PhenotypeMetaStore;
import edu.harvard.hms.dbmi.avillach.hpds.processing.util.SetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
public class PhenotypicQueryExecutor {

    private static Logger log = LoggerFactory.getLogger(PhenotypicQueryExecutor.class);

    private static final String CONSENTS_CONCEPT_PATH = "\\_consents\\";

    /**
     * Non-study-scoped concept-path roots that do not start with "_". These, plus any root
     * beginning with "_" (auxiliary concepts like _consents, _studies_consents, _VCF Sample Id,
     * and the various accession-index concepts), are excluded when computing the set of studies
     * referenced by a query. See {@link #isStudyScopedRoot(String)}.
     */
    private static final Set<String> NON_STUDY_NAMED_ROOTS = Set.of(
        "DCC Harmonized data set"
    );

    private final PhenotypeMetaStore phenotypeMetaStore;

    private final PhenotypicObservationStore phenotypicObservationStore;

    @Autowired
    public PhenotypicQueryExecutor(PhenotypeMetaStore phenotypeMetaStore, PhenotypicObservationStore phenotypicObservationStore) {
        this.phenotypeMetaStore = phenotypeMetaStore;
        this.phenotypicObservationStore = phenotypicObservationStore;
    }

    public Set<Integer> getPatientSet(Query query) {
        List<PhenotypicClause> mergedClauses = new ArrayList<>();

        Set<String> referencedStudies = referencedStudies(query);
        List<PhenotypicClause> authorizationClauses =
            authorizationFiltersToPhenotypicClause(query.authorizationFilters(), referencedStudies);
        mergedClauses.addAll(authorizationClauses);

        if (query.phenotypicClause() != null) {
            mergedClauses.add(query.phenotypicClause());
        }

        if (!mergedClauses.isEmpty()) {
            PhenotypicSubquery authorizedSubquery = new PhenotypicSubquery(null, mergedClauses, Operator.AND);
            return evaluatePhenotypicClause(authorizedSubquery);
        } else {
            // if there are no phenotypic queries, return all patients
            return phenotypeMetaStore.getPatientIds();
        }
    }

    /**
     * Converts the query's authorization filters into phenotypic clauses that will be AND-ed
     * with the user-supplied filters. The \_consents\ authorization filter is split into one
     * clause per study actually referenced by the query, each constrained to just the user's
     * consents for that study. This prevents cross-study leaks of patients who have overlapping
     * consents: a patient must have an allowed consent for EACH referenced study, not merely
     * for any study the user happens to be authorized for.
     *
     * Non-\_consents\ authorization filters (\_harmonized_consent\, \_topmed_consents\) are
     * passed through unchanged — they're already study-agnostic by design.
     */
    private List<PhenotypicClause> authorizationFiltersToPhenotypicClause(
        List<AuthorizationFilter> authorizationFilters, Set<String> referencedStudies
    ) {
        return authorizationFilters.stream().flatMap(authorizationFilter -> {
            if (CONSENTS_CONCEPT_PATH.equals(authorizationFilter.conceptPath())) {
                return segmentConsentsByStudy(authorizationFilter, referencedStudies).stream();
            }
            return Stream.<PhenotypicClause>of(
                new PhenotypicFilter(
                    PhenotypicFilterType.FILTER, authorizationFilter.conceptPath(), authorizationFilter.values(), null, null, null
                )
            );
        }).collect(Collectors.toList());
    }

    private List<PhenotypicClause> segmentConsentsByStudy(AuthorizationFilter authorizationFilter, Set<String> referencedStudies) {
        if (referencedStudies.isEmpty()) {
            // Query does not reference any study-scoped concepts (e.g. harmonized-only query).
            // The harmonized/topmed authorization filters handle those paths on their own.
            return List.of();
        }
        return referencedStudies.stream().<PhenotypicClause>map(study -> {
            Set<String> studyConsents = authorizationFilter.values().stream()
                .filter(consent -> consentBelongsToStudy(consent, study))
                .collect(Collectors.toSet());
            // If the user has zero consents for a referenced study, emit an empty-values filter.
            // The evaluator will return an empty patient set, causing the AND to correctly yield ∅.
            // (PSAMA's isConceptPathAuthorized should already have rejected such queries; this is
            // defense-in-depth.)
            return new PhenotypicFilter(
                PhenotypicFilterType.FILTER, CONSENTS_CONCEPT_PATH, studyConsents, null, null, null
            );
        }).collect(Collectors.toList());
    }

    /**
     * A consent value belongs to a study iff its first dot-delimited segment equals the study
     * accession. Mirrors PSAMA's BdcConsentBasedAccessRuleEvaluator logic: consent values are
     * either "<study>.<consentCode>" for dbGaP studies or a single token "<study>" for
     * single-consent/public studies that were added to the user's consents without a suffix.
     */
    private boolean consentBelongsToStudy(String consent, String study) {
        if (consent == null) {
            return false;
        }
        int dot = consent.indexOf('.');
        String consentStudy = dot < 0 ? consent : consent.substring(0, dot);
        return consentStudy.equals(study);
    }

    /**
     * Collects the set of study accessions actually referenced by the query. A study accession
     * is the first path segment of a concept path (e.g. "phs001001" in "\phs001001\data\sex\").
     * Non-study roots (harmonized data, and any underscore-prefixed auxiliary concept such as
     * _consents, _studies_consents, _VCF Sample Id, _Parent Study Accession with Subject ID,
     * etc.) are excluded — see {@link #isStudyScopedRoot(String)}.
     */
    private Set<String> referencedStudies(Query query) {
        Set<String> studies = new HashSet<>();
        query.allFilters().forEach(filter -> addStudyFromPath(filter.conceptPath(), studies));
        query.select().forEach(path -> addStudyFromPath(path, studies));
        return studies;
    }

    private void addStudyFromPath(String conceptPath, Set<String> studies) {
        if (conceptPath == null || conceptPath.isEmpty()) {
            return;
        }
        // concept paths start with a leading backslash, so split[0] is empty and split[1] is the root
        String[] split = conceptPath.split("\\\\");
        String root = split.length > 1 ? split[1] : split[0];
        if (isStudyScopedRoot(root)) {
            studies.add(root);
        }
    }

    private boolean isStudyScopedRoot(String root) {
        if (root == null || root.isEmpty()) {
            return false;
        }
        if (root.startsWith("_")) {
            return false;
        }
        return !NON_STUDY_NAMED_ROOTS.contains(root);
    }

    private Set<Integer> evaluatePhenotypicClause(PhenotypicClause phenotypicClause) {
        return switch (phenotypicClause) {
            case PhenotypicSubquery phenotypicSubquery -> evaluatePhenotypicSubquery(phenotypicSubquery);
            case PhenotypicFilter phenotypicFilter -> evaluatePhenotypicFilter(phenotypicFilter);
        };
    }

    private Set<Integer> evaluatePhenotypicFilter(PhenotypicFilter phenotypicFilter) {
        return switch (phenotypicFilter.phenotypicFilterType()) {
            case FILTER -> evaluateFilterFilter(phenotypicFilter);
            case REQUIRED -> evaluateRequiredFilter(phenotypicFilter);
            case ANY_RECORD_OF -> evaluateAnyRecordOfFilter(phenotypicFilter);
        };
    }

    private Set<Integer> evaluateAnyRecordOfFilter(PhenotypicFilter phenotypicFilter) {
        Set<String> matchingConcepts = phenotypeMetaStore.getChildConceptPaths(phenotypicFilter.conceptPath());
        Set<Integer> ids = new TreeSet<>();
        for (String concept : matchingConcepts) {
            ids.addAll(phenotypicObservationStore.getAllKeys(concept));
        }
        return ids;
    }

    private Set<Integer> evaluateFilterFilter(PhenotypicFilter phenotypicFilter) {
        if (phenotypicFilter.values() != null) {
            return phenotypicObservationStore.getKeysForValues(phenotypicFilter.conceptPath(), phenotypicFilter.values());
        } else if (phenotypicFilter.max() != null || phenotypicFilter.min() != null) {
            return phenotypicObservationStore
                .getKeysForRange(phenotypicFilter.conceptPath(), phenotypicFilter.min(), phenotypicFilter.max());
        } else {
            throw new IllegalArgumentException("Either values or one of min/max must be set for a filter");
        }
    }

    private Set<Integer> evaluateRequiredFilter(PhenotypicFilter phenotypicFilter) {
        return new HashSet<>(phenotypicObservationStore.getAllKeys(phenotypicFilter.conceptPath()));
    }

    private Set<Integer> evaluatePhenotypicSubquery(PhenotypicSubquery phenotypicSubquery) {
        return phenotypicSubquery.phenotypicClauses().parallelStream().map(this::evaluatePhenotypicClause)
            .reduce(getReducer(phenotypicSubquery.operator()))
            // todo: deal with empty lists
            .get();
    }

    private BinaryOperator<Set<Integer>> getReducer(Operator operator) {
        return switch (operator) {
            case OR -> SetUtils::union;
            case AND -> SetUtils::intersection;
        };
    }

    /**
     * If there are concepts in the list of paths which are already in the cache, push those to the front of the list so that we don't evict
     * and then reload them for concepts which are not yet in the cache.
     *
     * @param paths
     * @param columnCount
     * @return
     */
    public ArrayList<Integer> useResidentCubesFirst(List<String> paths, int columnCount) {
        int x;
        TreeSet<String> pathSet = new TreeSet<>(paths);
        Set<String> residentKeys = Sets.intersection(pathSet, phenotypicObservationStore.getCachedKeys());

        ArrayList<Integer> columnIndex = new ArrayList<Integer>();

        residentKeys.forEach(key -> {
            columnIndex.add(paths.indexOf(key) + 1);
        });

        Sets.difference(pathSet, residentKeys).forEach(key -> {
            columnIndex.add(paths.indexOf(key) + 1);
        });

        for (x = 1; x < columnCount; x++) {
            columnIndex.add(x);
        }
        return columnIndex;
    }


    public Map<String, ColumnMeta> getMetaStore() {
        return phenotypeMetaStore.getMetaStore();
    }

    public Set<Integer> getPatientIds() {
        return phenotypeMetaStore.getPatientIds();
    }
}

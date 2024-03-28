package edu.harvard.hms.dbmi.avillach.hpds.processing;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.KeyAndValue;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.PhenoCube;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.TimelineEvent;
import edu.harvard.hms.dbmi.avillach.hpds.data.query.Query;
import edu.harvard.hms.dbmi.avillach.hpds.exception.NotEnoughMemoryException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class TimelineProcessor implements HpdsProcessor {

	private final AbstractProcessor abstractProcessor;

	@Autowired
	public TimelineProcessor(AbstractProcessor abstractProcessor) {
		this.abstractProcessor = abstractProcessor;
	}

	@Override
	public void runQuery(Query query, AsyncResult asyncResult) throws NotEnoughMemoryException {
		throw new RuntimeException("Not yet implemented.");
	}

	public HashMap<String/* concept path */, List<TimelineEvent> /* events */> runTimelineQuery(Query query){

		// save the requiredFields and selected fields for later use
		List<String> requiredFieldsForTimeline = query.getRequiredFields();
		List<String> fieldsForTimeline = new ArrayList(query.getRequiredFields());
		fieldsForTimeline.addAll(query.getRequiredFields());

		// todo: copy the query?
		// wipe out required fields to not limit the patients by it
		query.setRequiredFields(new ArrayList<>());

		// list patients involved
		List<Integer> patientIds = new ArrayList<>(abstractProcessor.getPatientSubsetForQuery(query));
		patientIds.sort(Integer::compareTo);

		// get start time for the timeline
		long startTime = Long.MAX_VALUE;
		for(String field : requiredFieldsForTimeline) {
			PhenoCube cube = abstractProcessor.getCube(field);
			List<KeyAndValue> values = cube.getValuesForKeys(patientIds);
			for(KeyAndValue value : values) {
				if(value.getTimestamp()!=null && value.getTimestamp() > 0 && value.getTimestamp() < startTime) {
					startTime = value.getTimestamp();
				}
			}
		}
		final long _startTime = startTime;
		LinkedHashMap<String/* concept path */, List<TimelineEvent> /* events */> timelineEvents = 
				new LinkedHashMap<>();
		// fetch results for selected fields
		for(String concept : fieldsForTimeline) {
			PhenoCube cube = abstractProcessor.getCube(concept);
			List<KeyAndValue> values = cube.getValuesForKeys(patientIds);
			timelineEvents.put(concept, 
					values.parallelStream()
					.map( value->{
						return new TimelineEvent(value, _startTime);
					})
					.filter(event -> {
						return event.getTimestamp() > -1;
					})
					.sorted(TimelineEvent.timestampComparator)
					.collect(Collectors.toList()));
		}

		List<Entry<String, List<TimelineEvent>>> entries = new ArrayList<>(timelineEvents.entrySet());

		Collections.sort(entries, (a, b)->{
			if(a.getValue().isEmpty()) return 1;
			if(b.getValue().isEmpty()) return -1;
			return TimelineEvent.timestampComparator.compare(a.getValue().get(0), b.getValue().get(0));
		});

		timelineEvents.clear();
		entries.stream().forEach(
				(entry)->{
					timelineEvents.put(entry.getKey(), entry.getValue());
				});

		return timelineEvents;
	}

	public String[] getHeaderRow(Query query) {
		return null;
	}
}

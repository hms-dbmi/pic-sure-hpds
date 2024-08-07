package edu.harvard.hms.dbmi.avillach.hpds.service;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import javax.ws.rs.*;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;

import edu.harvard.hms.dbmi.avillach.hpds.data.query.ResultType;
import edu.harvard.hms.dbmi.avillach.hpds.service.filesharing.FileSharingService;
import edu.harvard.hms.dbmi.avillach.hpds.service.util.Paginator;
import edu.harvard.hms.dbmi.avillach.hpds.service.util.QueryDecorator;
import org.apache.http.entity.ContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.harvard.dbmi.avillach.domain.*;
import edu.harvard.dbmi.avillach.service.IResourceRS;
import edu.harvard.hms.dbmi.avillach.hpds.crypto.Crypto;
import edu.harvard.hms.dbmi.avillach.hpds.data.genotype.FileBackedByteIndexedInfoStore;
import edu.harvard.hms.dbmi.avillach.hpds.data.phenotype.ColumnMeta;
import edu.harvard.hms.dbmi.avillach.hpds.data.query.Query;
import edu.harvard.hms.dbmi.avillach.hpds.processing.*;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestBody;

@Path("PIC-SURE")
@Produces("application/json")
@Component("picSureService")
public class PicSureService implements IResourceRS {

	@Autowired
	public PicSureService(QueryService queryService, TimelineProcessor timelineProcessor, CountProcessor countProcessor,
						  VariantListProcessor variantListProcessor, AbstractProcessor abstractProcessor,
						  Paginator paginator, FileSharingService fileSystemService, QueryDecorator queryDecorator
	) {
		this.queryService = queryService;
		this.timelineProcessor = timelineProcessor;
		this.countProcessor = countProcessor;
		this.variantListProcessor = variantListProcessor;
		this.abstractProcessor = abstractProcessor;
		this.paginator = paginator;
		this.fileSystemService = fileSystemService;
		this.queryDecorator = queryDecorator;
		Crypto.loadDefaultKey();
	}

	private final QueryService queryService;

	private final ObjectMapper mapper = new ObjectMapper();

	private Logger log = LoggerFactory.getLogger(PicSureService.class);

	private final TimelineProcessor timelineProcessor;

	private final CountProcessor countProcessor;

	private final VariantListProcessor variantListProcessor;

	private final AbstractProcessor abstractProcessor;

	private final Paginator paginator;

	private final FileSharingService fileSystemService;

	private final QueryDecorator queryDecorator;

	private static final String QUERY_METADATA_FIELD = "queryMetadata";
	private static final int RESPONSE_CACHE_SIZE = 50;

	@POST
	@Path("/info")
	public ResourceInfo info(QueryRequest request) {
		ResourceInfo info = new ResourceInfo();
		info.setName("PhenoCube v1.0-SNAPSHOT");
		info.setId(UUID.randomUUID());

		try {
			info.setQueryFormats(ImmutableList.of(new QueryFormat().setDescription("PhenoCube Query Format")
					.setName("PhenoCube Query Format")
					.setExamples(ImmutableList.of(ImmutableMap.of(
							"Demographics and interesting variables for people with high blood pressure",
							new ObjectMapper().readValue(
									"{\"fields\":[\"\\\\demographics\\\\SEX\\\\\",\"\\\\demographics\\\\WTMEC2YR\\\\\",\"\\\\demographics\\\\WTMEC4YR\\\\\",\"\\\\demographics\\\\area\\\\\",\"\\\\demographics\\\\education\\\\\",\"\\\\examination\\\\blood pressure\\\\60 sec HR (30 sec HR * 2)\\\\\",\"\\\\examination\\\\blood pressure\\\\mean diastolic\\\\\",\"\\\\examination\\\\blood pressure\\\\mean systolic\\\\\",\"\\\\examination\\\\body measures\\\\Body Mass Index (kg per m**2)\\\\\",\"\\\\examination\\\\body measures\\\\Head BMD (g per cm^2)\\\\\",\"\\\\examination\\\\body measures\\\\Head Circumference (cm)\\\\\",\"\\\\examination\\\\body measures\\\\Lumber Pelvis BMD (g per cm^2)\\\\\",\"\\\\examination\\\\body measures\\\\Lumber Spine BMD (g per cm^2)\\\\\",\"\\\\examination\\\\body measures\\\\Maximal Calf Circumference (cm)\\\\\",\"\\\\examination\\\\body measures\\\\Recumbent Length (cm)\\\\\",\"\\\\examination\\\\body measures\\\\Standing Height (cm)\\\\\",\"\\\\examination\\\\body measures\\\\Subscapular Skinfold (mm)\\\\\"],"
											+ "\"numericFilters\":{\"\\\\examination\\\\blood pressure\\\\mean systolic\\\\\":{\"min\":120},\"\\\\examination\\\\blood pressure\\\\mean diastolic\\\\\":{\"min\":80}}}",
									Map.class)),
							ImmutableMap.of(
									"Demographics and interesting variables for men with high blood pressure who live with a smoker and for whom we have BMI data",
									ImmutableMap.of("fields",
											ImmutableList.of("\\demographics\\SEX\\", "\\demographics\\WTMEC2YR\\",
													"\\demographics\\WTMEC4YR\\", "\\demographics\\area\\",
													"\\demographics\\education\\",
													"\\examination\\blood pressure\\60 sec HR (30 sec HR * 2)\\",
													"\\examination\\blood pressure\\mean diastolic\\",
													"\\examination\\blood pressure\\mean systolic\\",
													"\\examination\\body measures\\Body Mass Index (kg per m**2)\\",
													"\\examination\\body measures\\Head BMD (g per cm^2)\\",
													"\\examination\\body measures\\Head Circumference (cm)\\",
													"\\examination\\body measures\\Lumber Pelvis BMD (g per cm^2)\\",
													"\\examination\\body measures\\Lumber Spine BMD (g per cm^2)\\",
													"\\examination\\body measures\\Maximal Calf Circumference (cm)\\",
													"\\examination\\body measures\\Recumbent Length (cm)\\",
													"\\examination\\body measures\\Standing Height (cm)\\",
													"\\examination\\body measures\\Subscapular Skinfold (mm)\\"),
											"requiredFields",
											ImmutableList.of(
													"\\examination\\body measures\\Body Mass Index (kg per m**2)\\"),
											"numericFilters",
											ImmutableMap.of("\\examination\\blood pressure\\mean systolic\\",
													ImmutableMap.of("min", 120),
													"\\examination\\blood pressure\\mean diastolic\\",
													ImmutableMap.of("min", 80)),
											"categoryFilters",
											ImmutableMap.of("\\demographics\\SEX\\", ImmutableList.of("male"),
													"\\questionnaire\\smoking family\\Does anyone smoke in home?\\",
													ImmutableList.of("Yes"))))))
					.setSpecification(ImmutableMap.of("fields",
							"A list of field names. Can be any key from the results map returned from the search endpoint of this resource. Unless filters are set, the included fields will be returned for all patients as a sparse matrix.",
							"numericFilters",
							"A map where each entry maps a field name to an object with min and/or max properties. Patients without a value between the min and max will not be included in the result set.",
							"requiredFields",
							"A list of field names for which a patient must have a value in order to be inclued in the result set.",
							"categoryFilters",
							"A map where each entry maps a field name to a list of values to be included in the result set."))));
		} catch (JsonParseException e) {
			log.error("JsonParseException  caught: ", e);
		} catch (JsonMappingException e) {
			log.error("JsonMappingException  caught: ", e);
		} catch (IOException e) {
			log.error("IOException  caught: ", e);
		}
		// TODO examples, examples, examples, specification
		return info;
	}

	@POST
	@Path("/search")
	public SearchResults search(QueryRequest searchJson) {
		Set<Entry<String, ColumnMeta>> allColumns = abstractProcessor.getDictionary().entrySet();

		// Phenotype Values
		Object phenotypeResults = searchJson.getQuery() != null ? allColumns.stream().filter((entry) -> {
			String lowerCaseSearchTerm = searchJson.getQuery().toString().toLowerCase();
			return entry.getKey().toLowerCase().contains(lowerCaseSearchTerm)
					|| (entry.getValue().isCategorical() && entry.getValue().getCategoryValues().stream()
							.map(String::toLowerCase).collect(Collectors.toList()).contains(lowerCaseSearchTerm));
		}).collect(Collectors.toMap(Entry::getKey, Entry::getValue)) : allColumns;

		// Info Values
		Map<String, Map> infoResults = new TreeMap<String, Map>();
		abstractProcessor.getInfoStoreColumns().stream().forEach((String infoColumn) -> {
			FileBackedByteIndexedInfoStore store = abstractProcessor.getInfoStore(infoColumn);
			if (store != null) {
				String query = searchJson.getQuery().toString();
				String lowerCase = query.toLowerCase();
				boolean storeIsNumeric = store.isContinuous;
				if (store.description.toLowerCase().contains(lowerCase)
						|| store.column_key.toLowerCase().contains(lowerCase)) {
					infoResults.put(infoColumn,
							ImmutableMap.of("description", store.description, "values",
									store.isContinuous ? new ArrayList<String>() : store.getAllValues().keys(), "continuous",
									storeIsNumeric));
				} else {
					List<String> searchResults = store.search(query);
					if (!searchResults.isEmpty()) {
						infoResults.put(infoColumn, ImmutableMap.of("description", store.description, "values",
								searchResults, "continuous", storeIsNumeric));
					}
				}
			}
		});

		return new SearchResults()
				.setResults(
						ImmutableMap.of("phenotypes", phenotypeResults, /* "genes", resultMap, */ "info", infoResults))
				.setSearchQuery(searchJson.getQuery().toString());
	}

	@POST
	@Path("/query")
	public QueryStatus query(QueryRequest queryJson) {
		if (Crypto.hasKey(Crypto.DEFAULT_KEY_NAME)) {
			try {
				Query query = convertIncomingQuery(queryJson);
				return convertToQueryStatus(queryService.runQuery(query));
			} catch (IOException e) {
				log.error("IOException caught in query processing:", e);
				throw new ServerErrorException(500);
			} catch (ClassNotFoundException e) {
				throw new ServerErrorException(500);
			}
		} else {
			QueryStatus status = new QueryStatus();
			status.setResourceStatus("Resource is locked.");
			return status;
		}
	}

	private Query convertIncomingQuery(QueryRequest queryJson)
			throws IOException, JsonParseException, JsonMappingException, JsonProcessingException {
		return mapper.readValue(mapper.writeValueAsString(queryJson.getQuery()), Query.class);
	}

	private QueryStatus convertToQueryStatus(AsyncResult entity) {
		QueryStatus status = new QueryStatus();
		status.setDuration(entity.completedTime == 0 ? 0 : entity.completedTime - entity.queuedTime);
		status.setResourceResultId(entity.id);
		status.setResourceStatus(entity.status.name());
		if (entity.status == AsyncResult.Status.SUCCESS) {
			status.setSizeInBytes(entity.stream.estimatedSize());
		}
		status.setStartTime(entity.queuedTime);
		status.setStatus(entity.status.toPicSureStatus());

		Map<String, Object> metadata = new HashMap<String, Object>();
		queryDecorator.setId(entity.query);
		metadata.put("picsureQueryId", entity.query.getId());
		status.setResultMetadata(metadata);
		return status;
	}

	@POST
	@Path("/query/{resourceQueryId}/result")
	@Produces(MediaType.TEXT_PLAIN_VALUE)
	@Override
	public Response queryResult(@PathParam("resourceQueryId") UUID queryId, QueryRequest resultRequest) {
		AsyncResult result = queryService.getResultFor(queryId.toString());
		if (result == null) {
			// This happens sometimes when users immediately request the status for a query
			// before it can be initialized. We wait a bit and try again before throwing an
			// error.
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				return Response.status(500).build();
			}

			result = queryService.getResultFor(queryId.toString());
			if (result == null) {
				return Response.status(404).build();
			}
		}
		if (result.status == AsyncResult.Status.SUCCESS) {
			result.stream.open();
			return Response.ok(result.stream).build();
		} else {
			return Response.status(400).entity("Status : " + result.status.name()).build();
		}
	}

	private Optional<String> roundTripUUID(String uuid) {
		try {
			return Optional.ofNullable(UUID.fromString(uuid).toString());
		} catch (IllegalArgumentException ignored) {
			return Optional.empty();
		}
	}

	@POST
	@Path("/write/{dataType}")
	public Response writeQueryResult(
		@RequestBody() Query query, @PathParam("dataType") String datatype
	) {
		if (roundTripUUID(query.getPicSureId()).map(id -> !id.equalsIgnoreCase(query.getPicSureId())).orElse(false)) {
			return Response
				.status(400, "The query pic-sure ID is not a UUID")
				.build();
		}
		if (query.getExpectedResultType() != ResultType.DATAFRAME_TIMESERIES) {
			return Response
				.status(400, "The write endpoint only writes time series dataframes. Fix result type.")
				.build();
		}
		String hpdsQueryID;
		try {
			QueryStatus queryStatus = convertToQueryStatus(queryService.runQuery(query));
			String status = queryStatus.getResourceStatus();
            hpdsQueryID = queryStatus.getResourceResultId();
            while ("RUNNING".equalsIgnoreCase(status) || "PENDING".equalsIgnoreCase(status)) {
				Thread.sleep(10000); // Yea, this is not restful. Sorry.
				status = convertToQueryStatus(queryService.getStatusFor(hpdsQueryID)).getResourceStatus();
			}
        } catch (ClassNotFoundException | IOException | InterruptedException e) {
			log.warn("Error waiting for response", e);
			return Response.serverError().build();
        }

		AsyncResult result = queryService.getResultFor(hpdsQueryID);
		// the queryResult has this DIY retry logic that blocks a system thread.
		// I'm not going to do that here. If the service can't find it, you get a 404.
		// Retry it client side.
		if (result == null) {
			return Response.status(404).build();
		}
		if (result.status == AsyncResult.Status.ERROR) {
			return Response.status(500).build();
		}
		if (result.status != AsyncResult.Status.SUCCESS) {
			return Response.status(503).build(); // 503 = unavailable
		}

		// at least for now, this is going to block until we finish writing
		// Not very restful, but it will make this API very easy to consume
		boolean success = false;
		query.setId(hpdsQueryID);
		if ("phenotypic".equals(datatype)) {
			success = fileSystemService.createPhenotypicData(query);
		} else if ("genomic".equals(datatype)) {
			success = fileSystemService.createGenomicData(query);
		}
		return success ? Response.ok().build() : Response.serverError().build();
	}

	@POST
	@Path("/query/{resourceQueryId}/status")
	@Override
	public QueryStatus queryStatus(@PathParam("resourceQueryId") UUID queryId, QueryRequest request) {
		return convertToQueryStatus(queryService.getStatusFor(queryId.toString()));
	}

	@POST
	@Path("/query/format")
	public Response queryFormat(QueryRequest resultRequest) {
		try {
			// The toString() method here has been overridden to produce a human readable
			// value
			return Response.ok().entity(convertIncomingQuery(resultRequest).toString()).build();
		} catch (IOException e) {
			return Response.ok()
				.entity("An error occurred formatting the query for display: " + e.getLocalizedMessage()).build();
		}
	}

	@POST
	@Path("/query/sync")
	@Produces(MediaType.TEXT_PLAIN_VALUE)
	public Response querySync(QueryRequest resultRequest) {
		if (Crypto.hasKey(Crypto.DEFAULT_KEY_NAME)) {
			try {
				return submitQueryAndWaitForCompletion(resultRequest);
			} catch (IOException e) {
				log.error("IOException  caught: ", e);
				return Response.serverError().build();
			}
		} else {
			return Response.status(403).entity("Resource is locked").build();
		}
	}

	@GET
	@Path("/search/values/")
	@Override
	public PaginatedSearchResult<String> searchGenomicConceptValues(
			@QueryParam("genomicConceptPath") String genomicConceptPath,
			@QueryParam("query") String query,
			@QueryParam("page") int page,
			@QueryParam("size") int size
	) {
		if (page < 1) {
			throw new IllegalArgumentException("Page must be greater than 0");
		}
		if (size < 1) {
			throw new IllegalArgumentException("Size must be greater than 0");
		}
		final List<String> matchingValues = abstractProcessor.searchInfoConceptValues(genomicConceptPath, query);
		return paginator.paginate(matchingValues, page, size);
	}

	private Response submitQueryAndWaitForCompletion(QueryRequest resultRequest) throws IOException {
		Query incomingQuery;
		incomingQuery = convertIncomingQuery(resultRequest);
		log.info("Query Converted");
		switch (incomingQuery.getExpectedResultType()) {

		case INFO_COLUMN_LISTING:
			ArrayList<Map> infoStores = new ArrayList<>();
			abstractProcessor.getInfoStoreColumns().stream().forEach((infoColumn) -> {
				FileBackedByteIndexedInfoStore store = abstractProcessor.getInfoStore(infoColumn);
				if (store != null) {
					infoStores.add(ImmutableMap.of("key", store.column_key, "description", store.description,
							"isContinuous", store.isContinuous, "min", store.min, "max", store.max));
				}
			});
			return Response.ok(infoStores, MediaType.APPLICATION_JSON_VALUE).build();

		case DATAFRAME:
		case SECRET_ADMIN_DATAFRAME:
		case DATAFRAME_TIMESERIES:
		case DATAFRAME_MERGED:
			QueryStatus status = query(resultRequest);
			while (status.getResourceStatus().equalsIgnoreCase("RUNNING")
					|| status.getResourceStatus().equalsIgnoreCase("PENDING")) {
				status = queryStatus(UUID.fromString(status.getResourceResultId()), null);
			}
			log.info(status.toString());

			AsyncResult result = queryService.getResultFor(status.getResourceResultId());
			if (result.status == AsyncResult.Status.SUCCESS) {
				result.stream.open();
				return queryOkResponse(result.stream, incomingQuery).build();
			}
			return Response.status(400).entity("Status : " + result.status.name()).build();

		case CROSS_COUNT:
			return queryOkResponse(countProcessor.runCrossCounts(incomingQuery), incomingQuery)
					.header(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON).build();

		case CATEGORICAL_CROSS_COUNT:
			return queryOkResponse(countProcessor.runCategoryCrossCounts(incomingQuery), incomingQuery)
					.header(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON).build();

		case CONTINUOUS_CROSS_COUNT:
			return queryOkResponse(countProcessor.runContinuousCrossCounts(incomingQuery), incomingQuery)
					.header(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON).build();

		case OBSERVATION_COUNT:
			return queryOkResponse(countProcessor.runObservationCount(incomingQuery), incomingQuery).build();

		case OBSERVATION_CROSS_COUNT:
			return queryOkResponse(countProcessor.runObservationCrossCounts(incomingQuery), incomingQuery)
					.header(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON).build();

		case VARIANT_COUNT_FOR_QUERY:
			return queryOkResponse(countProcessor.runVariantCount(incomingQuery), incomingQuery)
					.header(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON).build();

		case VARIANT_LIST_FOR_QUERY:
			return queryOkResponse(variantListProcessor.runVariantListQuery(incomingQuery), incomingQuery).build();

		case VCF_EXCERPT:
			return queryOkResponse(variantListProcessor.runVcfExcerptQuery(incomingQuery, true), incomingQuery).build();

		case AGGREGATE_VCF_EXCERPT:
			return queryOkResponse(variantListProcessor.runVcfExcerptQuery(incomingQuery, false), incomingQuery)
					.build();

		case TIMELINE_DATA:
			return queryOkResponse(mapper.writeValueAsString(timelineProcessor.runTimelineQuery(incomingQuery)),
					incomingQuery).build();

		case COUNT:
			return queryOkResponse(countProcessor.runCounts(incomingQuery), incomingQuery).build();

		default:
			// no valid type
			return Response.status(Status.BAD_REQUEST).build();
		}
	}

	private ResponseBuilder queryOkResponse(Object obj, Query incomingQuery) {
		queryDecorator.setId(incomingQuery);
		return Response.ok(obj).header(QUERY_METADATA_FIELD, incomingQuery.getId());
	}
}

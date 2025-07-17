package edu.harvard.hms.dbmi.avillach.hpds.service;

import edu.harvard.dbmi.avillach.util.UUIDv5;
import edu.harvard.hms.dbmi.avillach.hpds.data.query.ResultType;
import edu.harvard.hms.dbmi.avillach.hpds.data.query.v3.PhenotypicFilter;
import edu.harvard.hms.dbmi.avillach.hpds.data.query.v3.Query;
import edu.harvard.hms.dbmi.avillach.hpds.processing.dictionary.DictionaryService;
import edu.harvard.hms.dbmi.avillach.hpds.processing.io.CsvWriter;
import edu.harvard.hms.dbmi.avillach.hpds.processing.io.PfbWriter;
import edu.harvard.hms.dbmi.avillach.hpds.processing.io.ResultWriter;
import edu.harvard.hms.dbmi.avillach.hpds.processing.v3.*;
import edu.harvard.hms.dbmi.avillach.hpds.service.util.QueryV3Decorator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Service
public class QueryV3Service {

    private static final int RESULTS_CACHE_SIZE = 50;

    private final int SMALL_JOB_LIMIT;
    private final int LARGE_TASK_THREADS;
    private final int SMALL_TASK_THREADS;

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final BlockingQueue<Runnable> largeTaskExecutionQueue;

    private final ExecutorService largeTaskExecutor;

    private final BlockingQueue<Runnable> smallTaskExecutionQueue;

    private final ExecutorService smallTaskExecutor;

    private final QueryExecutor queryExecutor;
    private final QueryV3Processor queryProcessor;
    private final TimeseriesV3Processor timeseriesProcessor;
    private final CountV3Processor countProcessor;
    private final MultiValueQueryV3Processor multiValueQueryProcessor;
    private final PatientV3Processor patientProcessor;

    private final DictionaryService dictionaryService;
    private final QueryV3Decorator queryDecorator;

    private final QueryValidator queryValidator;

    HashMap<String, AsyncResult> results = new HashMap<>();


    @Autowired
    public QueryV3Service(
        QueryExecutor queryExecutor, QueryV3Processor queryProcessor, TimeseriesV3Processor timeseriesProcessor,
        CountV3Processor countProcessor, MultiValueQueryV3Processor multiValueQueryProcessor,
        @Autowired(required = false) DictionaryService dictionaryService, QueryV3Decorator queryDecorator, QueryValidator queryValidator,
        @Value("${SMALL_JOB_LIMIT}") Integer smallJobLimit, @Value("${SMALL_TASK_THREADS}") Integer smallTaskThreads,
        @Value("${LARGE_TASK_THREADS}") Integer largeTaskThreads, PatientV3Processor patientProcessor
    ) {
        this.queryExecutor = queryExecutor;
        this.queryProcessor = queryProcessor;
        this.timeseriesProcessor = timeseriesProcessor;
        this.countProcessor = countProcessor;
        this.multiValueQueryProcessor = multiValueQueryProcessor;
        this.dictionaryService = dictionaryService;
        this.queryDecorator = queryDecorator;
        this.queryValidator = queryValidator;

        SMALL_JOB_LIMIT = smallJobLimit;
        SMALL_TASK_THREADS = smallTaskThreads;
        LARGE_TASK_THREADS = largeTaskThreads;
        this.patientProcessor = patientProcessor;


        /*
         * These have to be of type Runnable(nothing more specific) in order to be compatible with ThreadPoolExecutor constructor prototype
         */
        largeTaskExecutionQueue = new PriorityBlockingQueue<Runnable>(1000);
        smallTaskExecutionQueue = new PriorityBlockingQueue<Runnable>(1000);

        largeTaskExecutor = createExecutor(largeTaskExecutionQueue, LARGE_TASK_THREADS);
        smallTaskExecutor = createExecutor(smallTaskExecutionQueue, SMALL_TASK_THREADS);
    }

    public AsyncResult runQuery(Query query) throws IOException {
        AsyncResult result = initializeResult(query);

        try {
            queryValidator.validate(query);
            if (query.select().size() > SMALL_JOB_LIMIT) {
                result.setJobQueue(largeTaskExecutor);
            } else {
                result.setJobQueue(smallTaskExecutor);
            }

            result.enqueue();
        } catch (IllegalArgumentException e) {
            result.setStatus(AsyncResult.Status.ERROR);
            return result;
        }
        return getStatusFor(result.getId());
    }

    ExecutorService countExecutor = Executors.newSingleThreadExecutor();

    public int runCount(Query query)
        throws InterruptedException, ExecutionException, ClassNotFoundException, FileNotFoundException, IOException {
        return countProcessor.runCounts(query);
    }

    private AsyncResult initializeResult(Query query) throws IOException {

        HpdsV3Processor p;
        switch (query.expectedResultType()) {
            case PATIENTS:
                p = patientProcessor;
                break;
            case SECRET_ADMIN_DATAFRAME:
                p = queryProcessor;
                break;
            case DATAFRAME_TIMESERIES:
                p = timeseriesProcessor;
                break;
            case COUNT:
            case CATEGORICAL_CROSS_COUNT:
            case CONTINUOUS_CROSS_COUNT:
                p = countProcessor;
                break;
            case DATAFRAME_PFB:
            case DATAFRAME:
                p = multiValueQueryProcessor;
                break;
            default:
                throw new RuntimeException("UNSUPPORTED RESULT TYPE");
        }

        String queryId = UUIDv5.UUIDFromString(query.toString()).toString();
        ResultWriter writer;
        if (ResultType.DATAFRAME_PFB.equals(query.expectedResultType())) {
            writer = new PfbWriter(File.createTempFile("result-" + System.nanoTime(), ".avro"), queryId, dictionaryService);
        } else {
            writer = new CsvWriter(File.createTempFile("result-" + System.nanoTime(), ".sstmp"));
        }

        queryDecorator.setId(query);
        AsyncResult result = new AsyncResult(query, p, writer).setStatus(AsyncResult.Status.PENDING)
            .setQueuedTime(System.currentTimeMillis()).setId(queryId);
        results.put(result.getId(), result);
        return result;
    }


    private List<String> includingOnlyDictionaryFields(Set<String> fields, Set<String> dictionaryFields) {
        return fields.stream().filter((value) -> {
            return dictionaryFields.contains(value);
        }).collect(Collectors.toList());
    }

    public AsyncResult getStatusFor(String queryId) {
        AsyncResult asyncResult = results.get(queryId);
        AsyncResult[] queue = asyncResult.getQuery().select().size() > SMALL_JOB_LIMIT
            ? largeTaskExecutionQueue.toArray(new AsyncResult[largeTaskExecutionQueue.size()])
            : smallTaskExecutionQueue.toArray(new AsyncResult[smallTaskExecutionQueue.size()]);
        if (asyncResult.getStatus() == AsyncResult.Status.PENDING) {
            ArrayList<AsyncResult> queueSnapshot = new ArrayList<AsyncResult>();
            for (int x = 0; x < queueSnapshot.size(); x++) {
                if (queueSnapshot.get(x).getId().equals(queryId)) {
                    asyncResult.setPositionInQueue(x);
                    break;
                }
            }
        } else {
            asyncResult.setPositionInQueue(-1);
        }
        asyncResult.setQueueDepth(queue.length);
        return asyncResult;
    }

    public AsyncResult getResultFor(String queryId) {
        return results.get(queryId);
    }

    private int getIntProp(String key) {
        return Integer.parseInt(System.getProperty(key));
    }

    private ExecutorService createExecutor(BlockingQueue<Runnable> taskQueue, int numThreads) {
        return new ThreadPoolExecutor(1, Math.max(2, numThreads), 10, TimeUnit.MINUTES, taskQueue);
    }

}

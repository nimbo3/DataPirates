package in.nimbo;

import com.codahale.metrics.*;
import com.codahale.metrics.jmx.JmxReporter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.MultiTermVectorsRequest;
import org.elasticsearch.client.core.TermVectorsRequest;
import org.elasticsearch.client.core.TermVectorsResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


public class ElasticDao implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(ElasticDao.class);
    private final String INDEX;
    private final Config config;
    private RestHighLevelClient client;
    private int SIZE = 10000;
    private long SCROLL_TIME_ALIVE = 10L;
    private static MetricRegistry metricRegistry;
    private static Timer updateTimer;
    private final Timer bulkInsertionTimer;
    private final Timer bulkTermVectorTimer;
    private final Meter bulkInsertionMeter;
    private final Meter bulkInsertionFailures;
    private final Counter docs_updated_counter;
    private BulkProcessor insertBulkProcessor;
    private int elasticBulkTimeOut;
    private BulkProcessor.Listener insertionBulkProcessorListener = new BulkProcessor.Listener() {
        Timer.Context bulkInsertTime;

        @Override
        public void beforeBulk(long executionId, BulkRequest request) {
            logger.info("Sending update bulk request ...");
            bulkInsertionMeter.mark(request.requests().size());
            bulkInsertTime = bulkInsertionTimer.time();
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
            logger.info("Update bulk request sent successfully.");
            bulkInsertTime.stop();
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
            logger.error("Update bulk request failed.", failure);
            bulkInsertTime.stop();
            bulkInsertionFailures.mark();
        }
    };

    public ElasticDao(Config config) {
        this.config = config;
        this.INDEX = config.getString("elastic.index.name");
        elasticBulkTimeOut = config.getInt("elastic.bulk.timeout");
        final RestClientBuilder builder = RestClient.builder(
                new HttpHost(config.getString("elastic.hostname"),
                        config.getInt("elastic.port")));
        builder.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
                .setConnectTimeout(config.getInt("elastic.connect.timeout")).
                        setSocketTimeout(config.getInt("elastic.socket.timeout"))
                .setConnectionRequestTimeout(config.getInt("elastic.connect.request.timeout")));
        client = new RestHighLevelClient(builder);

        BulkProcessor.Builder insertBulkProcessorBuilder = BulkProcessor.builder((bulkRequest, bulkResponseActionListener) ->
                client.bulkAsync(bulkRequest, RequestOptions.DEFAULT, bulkResponseActionListener), insertionBulkProcessorListener)
                .setBulkActions(config.getInt("elastic.bulk.size"))
                .setConcurrentRequests(config.getInt("elastic.concurrent.requests"))
                .setFlushInterval(TimeValue.timeValueMinutes(config.getInt("elastic.bulk.flush.interval.seconds")))
                .setBackoffPolicy(BackoffPolicy.constantBackoff(
                        TimeValue.timeValueSeconds(config.getLong("elastic.backoff.delay.seconds")),
                        config.getInt("elastic.backoff.retries")));

        insertBulkProcessor = insertBulkProcessorBuilder.build();
        metricRegistry = SharedMetricRegistries.getDefault();
        bulkInsertionMeter = metricRegistry.meter("elastic-bulk-update");
        bulkInsertionTimer = metricRegistry.timer("elastic-bulk-update-timer");
        bulkTermVectorTimer = metricRegistry.timer("elastic-bulk-term-vector-timer");
        bulkInsertionFailures = metricRegistry.meter("elastic-bulk-update-failure");
        updateTimer = metricRegistry.timer("update timer");
        docs_updated_counter = metricRegistry.counter("docs updated counter");
    }

    public void scroll() throws IOException {
        QueryBuilder matchQueryBuilder = QueryBuilders.matchAllQuery();
        SearchRequest searchRequest = new SearchRequest(INDEX);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(matchQueryBuilder);
        searchSourceBuilder.size(SIZE);
        searchRequest.source(searchSourceBuilder);
        searchRequest.scroll(TimeValue.timeValueMinutes(SCROLL_TIME_ALIVE));
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        String scrollId = searchResponse.getScrollId();
        do {
            String[] ids = Arrays.stream(searchResponse.getHits().getHits())
                    .map(SearchHit::getId).toArray(String[]::new);

            updateKeywordsWithId(ids);

            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(TimeValue.timeValueMinutes(SCROLL_TIME_ALIVE));
            searchResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
            scrollId = searchResponse.getScrollId();
        } while (searchResponse.getHits().getHits().length != 0);
        SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
    }

    public void updateKeywordsWithId(String[] ids) throws IOException {
        MultiTermVectorsRequest multiTermVectorRequest =
                new MultiTermVectorsRequest(ids, getTermVectorRequestSchema());
        final Timer.Context time = bulkTermVectorTimer.time();
        List<TermVectorsResponse> response = client.mtermvectors(multiTermVectorRequest,
                RequestOptions.DEFAULT).getTermVectorsResponses();
        time.stop();
        for (TermVectorsResponse termVectorsResponse : response) {
            String string = termVectorsResponse.getTermVectorsList().stream()
                    .flatMap(doc -> doc.getTerms().stream())
                    .map(TermVectorsResponse.TermVector.Term::getTerm)
                    .collect(Collectors.joining(","));
            update(string, termVectorsResponse.getId());
            docs_updated_counter.inc();
        }
    }

    public TermVectorsRequest getTermVectorRequestSchema() {
        TermVectorsRequest request = new TermVectorsRequest(INDEX, "fake");
        request.setFields("text");
        request.setTermStatistics(true);
        request.setFieldStatistics(true);
        request.setPositions(false);
        request.setOffsets(false);
        Map<String, Integer> filters = new HashMap<>();
        filters.put("max_num_terms", 20);
        request.setFilterSettings(filters);
        return request;
//        TermVectorsResponse response = client.termvectors(request, RequestOptions.DEFAULT);
//        for (TermVectorsResponse.TermVector tv : response.getTermVectorsList()) {
//            if (tv.getTerms() != null) {
//                List<TermVectorsResponse.TermVector.Term> terms =
//                        tv.getTerms();
//                for (TermVectorsResponse.TermVector.Term term : terms) {
//                    String termStr = term.getTerm();
//                    stringBuilder.append(termStr).append(", ");
//                }
//            }
//        }
//        if(stringBuilder.length() >= 2 && stringBuilder.charAt(stringBuilder.length() - 2) == ',')
//            stringBuilder.delete(stringBuilder.length() - 2, stringBuilder.length());
//        return stringBuilder.toString();
    }

    public static void main(String[] args) {
        SharedMetricRegistries.setDefault("data-pirates-keywords");
        JmxReporter jmxReporter = JmxReporter.forRegistry(metricRegistry).inDomain("keyword-extractor").build();
        jmxReporter.start();
        ElasticDao elasticDao = null;
        try {
            elasticDao = new ElasticDao(ConfigFactory.load("config"));
            ShutDownHook shutDownHook = new ShutDownHook(elasticDao);
            Runtime.getRuntime().addShutdownHook(shutDownHook);
            elasticDao.scroll();
        } catch (IOException e) {
            logger.error("can't search query to elastic", e);
        } finally {
            try {
                assert elasticDao != null;
                elasticDao.close();
            } catch (IOException e) {
                logger.error("can't close connection to elasticsearch", e);
            }

        }
    }

    public void update(String string, String id){
        UpdateRequest request = new UpdateRequest(INDEX, id);
        String jsonString = "{" +
                "\"tags\":\"" + string + "\"" +
                "}";
        request.doc(jsonString, XContentType.JSON);
        insertBulkProcessor.add(request);
    }

    @Override
    public void close() throws IOException {
        try {
            insertBulkProcessor.awaitClose(elasticBulkTimeOut, TimeUnit.SECONDS);
            client.close();
        } catch (InterruptedException e) {
            logger.error("Thread interrupted in elastic close.", e);
            Thread.currentThread().interrupt();
        }
    }
}

package in.nimbo;

import com.codahale.metrics.*;
import com.codahale.metrics.Timer;
import com.codahale.metrics.jmx.JmxReporter;
import com.fasterxml.jackson.core.JsonParseException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
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
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


public class ElasticDao implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(ElasticDao.class);
    private static boolean closed = false;
    private final String INDEX;
    private final Config config;
    private final Timer bulkInsertionTimer;
    private final Timer bulkTermVectorTimer;
    private final Meter bulkInsertionMeter;
    private final Meter bulkInsertionFailures;
    private final Meter updateKeywordsWithIdFailure;
    private final Counter docs_updated_counter;
    private final Counter docs_skipped_update_counter;
    private final Counter docs_with_so_many_numbers_counter;
    private RestHighLevelClient client;
    private int SIZE = 10000;
    private int MAX_NUM_TERMS = 20;
    private int NUM_SAVED_KEYWORDS = 10;
    private int MIN_DOC_FREQ = 3;
    private long SCROLL_TIME_ALIVE = 10L;
    private String scrollId = "";
    private BulkProcessor insertBulkProcessor;
    private int elasticBulkTimeOut;

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

        BulkProcessor.Listener insertionBulkProcessorListener = new BulkProcessor.Listener() {
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

        BulkProcessor.Builder insertBulkProcessorBuilder =
                BulkProcessor.builder(
                        (bulkRequest, bulkResponseActionListener) ->
                                client.bulkAsync(
                                        bulkRequest, RequestOptions.DEFAULT, bulkResponseActionListener
                                ), insertionBulkProcessorListener
                )
                        .setBulkActions(config.getInt("elastic.bulk.size"))
                        .setConcurrentRequests(config.getInt("elastic.concurrent.requests"))
                        .setFlushInterval(TimeValue.timeValueMinutes(
                                config.getInt("elastic.bulk.flush.interval.seconds")
                                )
                        ).setBackoffPolicy(BackoffPolicy.constantBackoff(
                        TimeValue.timeValueSeconds(
                                config.getLong("elastic.backoff.delay.seconds")),
                        config.getInt("elastic.backoff.retries")
                        )
                );
        insertBulkProcessor = insertBulkProcessorBuilder.build();
        //initializing metrics
        MetricRegistry metricRegistry = SharedMetricRegistries.getDefault();
        JmxReporter jmxReporter =
                JmxReporter.forRegistry(metricRegistry).inDomain("keyword-extractor").build();
        jmxReporter.start();
        bulkInsertionMeter = metricRegistry.meter("elastic-bulk-update");
        bulkInsertionTimer = metricRegistry.timer("elastic-bulk-update-timer");
        bulkTermVectorTimer = metricRegistry.timer("elastic-bulk-term-vector-timer");
        bulkInsertionFailures = metricRegistry.meter("elastic-bulk-update-failure");
        updateKeywordsWithIdFailure = metricRegistry.meter("update-keywords-with-id-failure");
        docs_updated_counter = metricRegistry.counter("docs updated counter");
        docs_skipped_update_counter = metricRegistry.counter("docs skipped update counter");
        docs_with_so_many_numbers_counter = metricRegistry.counter("docs with so many numbers counter");
    }

    public static void main(String[] args) {
        SharedMetricRegistries.setDefault("data-pirates-keywords");
        ElasticDao elasticDao = null;
        try {
            elasticDao = new ElasticDao(ConfigFactory.load("config"));
            ShutDownHook shutDownHook = new ShutDownHook(elasticDao);
            Runtime.getRuntime().addShutdownHook(shutDownHook);
            while (!closed) {
                try {
                    elasticDao.scroll();
                } catch (IOException e) {
                    logger.error("can't search query to elastic", e);
                } catch (Exception e) {
                    logger.error("Exception in Thread main", e);
                }
            }
        } finally {
            try {
                assert elasticDao != null;
                elasticDao.close();
            } catch (IOException e) {
                logger.error("can't close connection to elasticsearch", e);
            }

        }
    }

    public void scroll() throws IOException {
        SearchResponse searchResponse;
        if (scrollId.length() == 0) {
            QueryBuilder matchQueryBuilder = QueryBuilders.matchAllQuery();
            SearchRequest searchRequest = new SearchRequest(INDEX);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(matchQueryBuilder);
            searchSourceBuilder.size(SIZE);
            searchRequest.source(searchSourceBuilder);
            searchRequest.scroll(TimeValue.timeValueMinutes(SCROLL_TIME_ALIVE));
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            scrollId = searchResponse.getScrollId();
        } else {
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(TimeValue.timeValueMinutes(SCROLL_TIME_ALIVE));
            searchResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
            scrollId = searchResponse.getScrollId();
        }
        do {
            int docsWithSoManyNumbers = 0;
            List<String> list = new LinkedList<>();
            for (SearchHit hit : searchResponse.getHits().getHits()) {
                if (hit.getSourceAsMap().containsKey("tags")) {
                    String tag = hit.getSourceAsMap().get("tags").toString();
                    if (hasSoManyNumbers(tag)) {
                        ++docsWithSoManyNumbers;
                        list.add(hit.getId());
                    }
                    if (tag.split(" ").length > NUM_SAVED_KEYWORDS) {
                        list.add(hit.getId());
                    }
                } else {
                    list.add(hit.getId());
                }
            }

            final String[] ids = list.toArray(new String[0]);

            docs_skipped_update_counter.inc(searchResponse.getHits().getHits().length - ids.length);
            docs_with_so_many_numbers_counter.inc(docsWithSoManyNumbers);

            try {
                if(ids.length > 0) {
                    updateKeywordsWithId(ids);
                }
            } catch (JsonParseException | SocketTimeoutException e) {
                logger.error("can't update keywords", e);
                updateKeywordsWithIdFailure.mark();
            }catch (ElasticsearchStatusException e){
                logger.error("ids requested is empty", e);
            }

            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(TimeValue.timeValueMinutes(SCROLL_TIME_ALIVE));
            searchResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
            scrollId = searchResponse.getScrollId();
        } while (searchResponse.getHits().getHits().length != 0);
        scrollId = "";
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
            if (hasSoManyNumbers(string)) {
                string = getSingleTermVector(termVectorsResponse.getId());
            }
            update(string, termVectorsResponse.getId());
            docs_updated_counter.inc();
        }
    }

    private boolean hasSoManyNumbers(String string) {
        return Arrays.stream(string.split(" "))
                .filter(word -> word.matches("\\d+")).count() > (double) MAX_NUM_TERMS * 0.75;
    }

    public TermVectorsRequest getTermVectorRequestSchema() {
        TermVectorsRequest request = new TermVectorsRequest(INDEX, "fake");
        request.setFields("text");
        request.setTermStatistics(true);
        request.setFieldStatistics(true);
        request.setPositions(false);
        request.setOffsets(false);
        Map<String, Integer> filters = new HashMap<>();
        filters.put("max_num_terms", MAX_NUM_TERMS);
        filters.put("min_doc_freq", MIN_DOC_FREQ);
        request.setFilterSettings(filters);
        return request;
    }

    public String getSingleTermVector(String id) throws IOException {
        StringBuilder stringBuilder = new StringBuilder();
        TermVectorsRequest request = new TermVectorsRequest(INDEX, id);
        request.setFields("text");
        request.setTermStatistics(true);
        request.setFieldStatistics(true);
        request.setPositions(false);
        request.setOffsets(false);
        GetRequest getRequest = new GetRequest(INDEX, id);
        String text = "";
        String link = "";
        try {
            GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
            if (response.isExists()) {
                text = response.getSourceAsMap().get("text").toString();
                link = response.getSourceAsMap().get("link").toString();
            } else {
                logger.warn(String.format("Elastic found no match id for [%s]", link));
            }
        } catch (IOException e) {
            logger.error(String.format("Elastic couldn't get [%s]", link), e);
        }
        Map<String, Integer> filters = new HashMap<>();
        int MAX_NUM_TERMS = text.split(" ").length;
        filters.put("max_num_terms", MAX_NUM_TERMS);
        filters.put("min_doc_freq", MIN_DOC_FREQ);
        request.setFilterSettings(filters);
        TermVectorsResponse response = client.termvectors(request, RequestOptions.DEFAULT);
        for (TermVectorsResponse.TermVector tv : response.getTermVectorsList()) {
            if (tv.getTerms() != null) {
                List<TermVectorsResponse.TermVector.Term> terms = tv.getTerms();
                terms.sort(Comparator.comparing(TermVectorsResponse.TermVector.Term::getScore).reversed());
                int count = 0;
                for (TermVectorsResponse.TermVector.Term term : terms) {
                    if (count >= NUM_SAVED_KEYWORDS)
                        break;
                    String termStr = term.getTerm();
                    if (!termStr.matches("\\d+")) {
                        ++count;
                        stringBuilder.append(termStr).append(", ");
                    }
                }
            }
        }
        if (stringBuilder.length() >= 2 && stringBuilder.charAt(stringBuilder.length() - 2) == ',')
            stringBuilder.delete(stringBuilder.length() - 2, stringBuilder.length());
        return stringBuilder.toString();
    }

    public void update(String string, String id) {
        UpdateRequest request = new UpdateRequest(INDEX, id).doc("tags", string);
        insertBulkProcessor.add(request);
    }

    @Override
    public void close() throws IOException {
        try {
            closed = true;
            insertBulkProcessor.awaitClose(elasticBulkTimeOut, TimeUnit.SECONDS);
            client.close();
        } catch (InterruptedException e) {
            logger.error("Thread interrupted in elastic close.", e);
            Thread.currentThread().interrupt();
        }
    }
}

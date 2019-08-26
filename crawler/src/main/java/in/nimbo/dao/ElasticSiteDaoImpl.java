package in.nimbo.dao;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.typesafe.config.Config;
import in.nimbo.exception.ElasticSiteDaoException;
import in.nimbo.model.Site;
import in.nimbo.util.HashCodeGenerator;
import org.apache.http.HttpHost;
import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class ElasticSiteDaoImpl implements SiteDao, Closeable {
    private final Logger logger = Logger.getLogger(ElasticSiteDaoImpl.class);
    private final String INDEX;
    private final Timer insertionTimer;
    private final Meter elasticFailureMeter;
    private final Timer deleteTimer;
    private final Timer bulkInsertionTimer;
    private final Meter bulkInsertionMeter;
    private final Meter bulkInsertionFailures;
    private int elasticBulkTimeOut;
    private RestHighLevelClient client;
    private BulkProcessor bulkProcessor;

    private BulkProcessor.Listener bulkProcessorListener = new BulkProcessor.Listener() {
        Timer.Context bulkInsertTime;

        @Override
        public void beforeBulk(long executionId, BulkRequest request) {
            logger.info("Sending bulk request ...");
            bulkInsertionMeter.mark(request.requests().size());
            bulkInsertTime = bulkInsertionTimer.time();
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
            logger.info("Bulk request sent successfully.");
            bulkInsertTime.stop();
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
            logger.error("Bulk request failed.", failure);
            bulkInsertTime.stop();
            bulkInsertionFailures.mark();
        }
    };

    public ElasticSiteDaoImpl(Config config) {
        this.INDEX = "sites";
        this.elasticBulkTimeOut = config.getInt("elastic.bulk.timeout");
        this.client = new RestHighLevelClient(RestClient.builder(
                new HttpHost(config.getString("elastic.hostname"),
                        config.getInt("elastic.port"))));

        BulkProcessor.Builder bulkProcessorBuilder = BulkProcessor.builder((bulkRequest, bulkResponseActionListener) ->
                        client.bulkAsync(bulkRequest, RequestOptions.DEFAULT, bulkResponseActionListener), bulkProcessorListener)
            .setBulkActions(config.getInt("elastic.bulk.size"))
            .setConcurrentRequests(config.getInt("elastic.concurrent.requests"))
            .setFlushInterval(TimeValue.timeValueMinutes(config.getInt("elastic.bulk.flush.interval.seconds")))
            .setBackoffPolicy(BackoffPolicy.constantBackoff(
                    TimeValue.timeValueSeconds(config.getLong("elastic.backoff.delay.seconds")),
                    config.getInt("elastic.backoff.retries")));
        bulkProcessor = bulkProcessorBuilder.build();

        MetricRegistry metricRegistry = SharedMetricRegistries.getDefault();
        deleteTimer = metricRegistry.timer("elastic-delete");
        insertionTimer = metricRegistry.timer("elastic-insertion");
        elasticFailureMeter = metricRegistry.meter("elastic-insertion-failure");
        bulkInsertionMeter = metricRegistry.meter("elastic-bulk-insertion");
        bulkInsertionTimer = metricRegistry.timer("elastic-bulk-insertion-timer");
        bulkInsertionFailures = metricRegistry.meter("elastic-bulk-insertion-failure");
    }

    public Site get(Site site) {
        String hashedUrl = HashCodeGenerator.sha2Hash(site.getNoProtocolLink());
        GetRequest getRequest = new GetRequest(String.format("%s-%s", INDEX, site.getLanguage()), hashedUrl);
        try {
            GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
            if (response.isExists()) {
                return new Site(
                        response.getSourceAsMap().get("link").toString(),
                        response.getSourceAsMap().get("title").toString());
            } else {
                logger.warn(String.format("Elastic found no match id for [%s]", site.getLink()));
                return null;
            }
        } catch (IOException e) {
            logger.error(String.format("Elastic couldn't get [%s]", site.getLink()), e);
            return null;
        }
    }

    @Override
    public void insert(Site site) throws ElasticSiteDaoException {
        try (Timer.Context time = insertionTimer.time()) {
            String hashedUrl = HashCodeGenerator.sha2Hash(site.getNoProtocolLink());
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            builder.field("title", site.getTitle());
            builder.field("link", site.getLink());
            builder.field("text", site.getPlainText());
            builder.field("keywords", site.getKeywords());
            builder.field("domain", site.getDomain());
            builder.endObject();
            IndexRequest indexRequest = new IndexRequest(String.format("%s-%s", INDEX, site.getLanguage()))
                    .id(hashedUrl).source(builder);
            bulkProcessor.add(indexRequest);
            logger.trace(String.format("Elastic Inserted [%s]", site.getLink()));
        } catch (IOException e) {
            elasticFailureMeter.mark();
            throw new ElasticSiteDaoException(String.format("Elastic couldn't insert [%s]", site.getLink()), e);
        }
    }

    @Override
    public void delete(Site site) {
        String hashedUrl = HashCodeGenerator.sha2Hash(site.getNoProtocolLink());
        DeleteRequest deleteRequest = new DeleteRequest(String.format("%s-%s", INDEX, site.getLanguage()), hashedUrl);
        try (Timer.Context time = deleteTimer.time()) {
            client.delete(deleteRequest, RequestOptions.DEFAULT);
            logger.trace(String.format("Link [%s] deleted from elastic", site.getLink()));
        } catch (IOException e) {
            logger.error(String.format("Elastic couldn't delete [%s]", site.getLink()), e);
        }
    }


    @Override
    public void close() throws IOException {
        try {
            logger.trace("Shut down operation in elastic started ...");
            bulkProcessor.awaitClose(elasticBulkTimeOut, TimeUnit.SECONDS);
            client.close();
            logger.trace("Shut down operation in elastic completed.");
        } catch (InterruptedException e) {
            logger.error("Thread interrupted in elastic close.", e);
            Thread.currentThread().interrupt();
        }
    }
}

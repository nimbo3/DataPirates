package in.nimbo.dao;

import com.codahale.metrics.Meter;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.typesafe.config.Config;
import in.nimbo.exception.ElasticSiteDaoException;
import in.nimbo.exception.ElasticLongIdException;
import in.nimbo.model.Site;
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
    private static Logger logger = Logger.getLogger(ElasticSiteDaoImpl.class);
    private final Config config;
    private final Timer insertionTimer;
    private final Meter elasticFailureMeter;
    private final Timer deleteTimer;
    private RestHighLevelClient restHighLevelClient;
    private BulkProcessor bulkProcessor;
    private String index;
    private String hostname;
    private int port;

    private BulkProcessor.Listener listener = new BulkProcessor.Listener() {
        @Override
        public void beforeBulk(long executionId, BulkRequest request) {
            logger.info("Sending bulk request ...");
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
            //Todo : Handle failures in response
            logger.info("Bulk request sent.");
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
            logger.error("Bulk request failed !", failure);
        }
    };

    public ElasticSiteDaoImpl(Config config) {
        this.config = config;
        insertionTimer = SharedMetricRegistries.getDefault().timer(config.getString("elastic.insertion.metric.name"));
        elasticFailureMeter = SharedMetricRegistries.getDefault().meter(config.getString("elastic.insertion.failure.metric.name"));
        deleteTimer = SharedMetricRegistries.getDefault().timer(config.getString("elastic.delete.metric.name"));
        this.hostname = config.getString("elastic.hostname");
        this.port = config.getInt("elastic.port");
        this.index = "sites";
        BulkProcessor.Builder bulkProcessorBuilder = BulkProcessor.builder(
                (bulkRequest, bulkResponseActionListener) ->
                        getClient().bulkAsync(bulkRequest, RequestOptions.DEFAULT,
                                bulkResponseActionListener), listener);
        bulkProcessorBuilder.setBulkActions(config.getInt("elastic.bulk.size"));
        bulkProcessorBuilder.setConcurrentRequests(config.getInt("elastic.concurrent.requests"));
        bulkProcessorBuilder.setFlushInterval(
                TimeValue.timeValueMinutes(config.getInt("elastic.bulk.flush.interval.seconds")));
        bulkProcessorBuilder.setBackoffPolicy(BackoffPolicy.constantBackoff(
                TimeValue.timeValueSeconds(config.getLong("elastic.backoff.delay.seconds")),
                config.getInt("elastic.backoff.retries")));
        bulkProcessor = bulkProcessorBuilder.build();
    }

    private RestHighLevelClient getClient() {
        if (restHighLevelClient == null) {
            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(hostname, port)));
        }
        return restHighLevelClient;
    }


    public Site get(String url) {
        GetRequest getRequest = new GetRequest(index, url);
        try {
            GetResponse response = getClient().get(getRequest, RequestOptions.DEFAULT);
            if (response.isExists()) {
                return new Site(
                        response.getId(),
                        response.getSourceAsMap().get("title").toString());
            } else {
                return null;
            }
        } catch (IOException e) {
            logger.error(String.format("Elastic couldn't get [%s]", url), e);
            return null;
        }
    }

    @Override
    public void insert(Site site) throws ElasticSiteDaoException {
        try (Timer.Context time = insertionTimer.time()) {
            if (site.getLink().getBytes().length >= 512)
                throw new ElasticLongIdException("Elastic Long Id Exception (bytes of id must be lower than 512 bytes)");
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            builder.field("title", site.getTitle());
            builder.field("text", site.getPlainText());
            builder.field("keywords", site.getKeywords());
            builder.endObject();
            IndexRequest indexRequest = new IndexRequest(index).id(site.getLink()).source(builder);
            bulkProcessor.add(indexRequest);
        } catch (IOException e) {
            elasticFailureMeter.mark();
            throw new ElasticSiteDaoException(String.format("Elastic couldn't insert [%s]", site.getLink()), e);
        }
    }

    @Override
    public void delete(String url) {
        DeleteRequest deleteRequest = new DeleteRequest(index, url);
        try (Timer.Context time = deleteTimer.time()) {
            getClient().delete(deleteRequest, RequestOptions.DEFAULT);
            logger.debug(String.format("Link [%s] deleted from elastic", url));
        } catch (IOException e) {
            logger.error(String.format("Elastic couldn't delete [%s]", url), e);
        }
    }


    @Override
    public void close() throws IOException {
        try {
            bulkProcessor.awaitClose(config.getInt("elastic.bulk.timeout"), TimeUnit.SECONDS);
            getClient().close();
        } catch (InterruptedException e) {
            logger.error("thread interrupted in elastic close.", e);
        }
    }
}

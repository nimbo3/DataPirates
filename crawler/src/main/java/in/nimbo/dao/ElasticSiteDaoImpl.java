package in.nimbo.dao;

import com.codahale.metrics.Meter;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.typesafe.config.Config;
import in.nimbo.exception.SiteDaoException;
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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class ElasticSiteDaoImpl implements SiteDao {
    private static Logger logger = Logger.getLogger(ElasticSiteDaoImpl.class);
    private Timer insertionTimer = SharedMetricRegistries.getDefault().timer("elastic-insertion");
    private Meter elasticFailureMeter = SharedMetricRegistries.getDefault().meter("elastic-insertion-failure");
    private Timer deleteTimer = SharedMetricRegistries.getDefault().timer("elastic-delete");
    private RestHighLevelClient client;
    private BulkProcessor bulkProcessor;
    private String index;

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
        this.index = config.getString("elastic.index");
        BulkProcessor.Builder bulkProcessorBuilder = BulkProcessor.builder(
                (bulkRequest, bulkResponseActionListener) ->
                        client.bulkAsync(bulkRequest, RequestOptions.DEFAULT,
                                bulkResponseActionListener), listener);
        bulkProcessorBuilder.setBulkActions(config.getInt("elastic.bulk.actions.size"));
        bulkProcessorBuilder.setConcurrentRequests(config.getInt("elastic.concurrent.requests"));
        bulkProcessorBuilder.setFlushInterval(
                TimeValue.timeValueMinutes(config.getInt("elastic.bulk.flush.interval.seconds")));
        bulkProcessorBuilder.setBackoffPolicy(BackoffPolicy.constantBackoff(
                TimeValue.timeValueSeconds(config.getLong("elastic.backoff.delay.seconds")),
                config.getInt("elastic.backoff.retries")));
        bulkProcessor = bulkProcessorBuilder.build();
        client = new RestHighLevelClient(
                RestClient.builder(new HttpHost(
                        config.getString("elastic.hostname"),
                        config.getInt("elastic.port"), "http")));
    }

    @Override
    public void insert(Site site) throws SiteDaoException {
        try (Timer.Context time = insertionTimer.time()) {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            builder.field("title", site.getTitle());
            builder.field("metadata", site.getMetadata());
            builder.field("text", site.getPlainText());
            builder.field("keywords", site.getKeywords());
            builder.endObject();
            IndexRequest indexRequest = new IndexRequest(index).id(site.getLink()).source(builder);
            bulkProcessor.add(indexRequest);
        } catch (IOException e) {
            elasticFailureMeter.mark();
            logger.error(String.format("Elastic couldn't insert [%s]", site.getLink()), e);
            throw new SiteDaoException(e);
        }
    }

    public Site get(String url) {
        GetRequest getRequest = new GetRequest(index, url);
        try {
            GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
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
    public void delete(String url) {
        DeleteRequest deleteRequest = new DeleteRequest(index, url);
        try (Timer.Context time = deleteTimer.time()) {
            client.delete(deleteRequest, RequestOptions.DEFAULT);
            logger.info(String.format("Link [%s] deleted from hbase", url));
        } catch (IOException e) {
            logger.error(String.format("Elastic couldn't delete [%s]", url), e);
        }
    }

    public void stop() throws Exception {
        bulkProcessor.awaitClose(30, TimeUnit.SECONDS);
        client.close();
    }

    @Override
    public void close() throws IOException {

    }
}

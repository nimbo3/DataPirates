package in.nimbo.database.dao;

import com.codahale.metrics.Meter;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.typesafe.config.Config;
import in.nimbo.database.Searchable;
import in.nimbo.exception.SiteDaoException;
import in.nimbo.model.SearchResult;
import in.nimbo.model.Site;
import org.apache.http.HttpHost;
import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ElasticSiteDaoImpl implements SiteDao, Searchable {
    private static Logger logger = Logger.getLogger(ElasticSiteDaoImpl.class);
    private Timer insertionTimer = SharedMetricRegistries.getDefault().timer("elastic-insertion");
    private Meter elasticFailureMeter = SharedMetricRegistries.getDefault().meter("elastic-insertion-failure");
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
        this.hostname = config.getString("elastic.hostname");
        this.port = config.getInt("elastic.port");
        this.index = config.getString("elastic.index");
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
                    RestClient.builder(new HttpHost(hostname, port, "http")));
        }
        return restHighLevelClient;
    }

    @Override
    public List<SearchResult> search(String search) {
        SearchRequest searchRequest = new SearchRequest("sites");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.termQuery("metadata", search));
        searchSourceBuilder.query(QueryBuilders.termQuery("keywords", search));
        searchSourceBuilder.query(QueryBuilders.termQuery("title", search));
        searchSourceBuilder.query(QueryBuilders.termQuery("text", search));
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse;
        try {
            RestHighLevelClient client = getClient();
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            List<SearchResult> searchResults = new ArrayList<>();
            for (SearchHit searchHit : searchResponse.getHits().getHits()) {
                SearchResult searchResult = new SearchResult();
                searchResult.setLink(searchHit.getId());
                searchResult.setTitle(searchHit.getSourceAsMap().get("title").toString());
                searchResults.add(searchResult);
            }
            return searchResults;
        } catch (IOException e) {
            logger.error(String.format("Elastic couldn't search [%s]", search), e);
            return null;
        }
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

    public void delete(String url) {
        DeleteRequest deleteRequest = new DeleteRequest(index, url);
        try {
            getClient().delete(deleteRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            logger.error(String.format("Elastic couldn't delete [%s]", url), e);
        }
    }

    public void stop() throws Exception {
        bulkProcessor.awaitClose(30, TimeUnit.SECONDS);
        getClient().close();
    }
}

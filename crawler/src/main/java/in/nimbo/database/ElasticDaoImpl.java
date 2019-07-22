package in.nimbo.database;

import in.nimbo.database.dao.SiteDao;
import in.nimbo.model.SearchResult;
import in.nimbo.model.Site;
import org.apache.http.HttpHost;
import org.apache.log4j.Logger;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ElasticDaoImpl implements SiteDao, Searchable {
    private static Logger logger = Logger.getLogger(ElasticDaoImpl.class);
    private RestHighLevelClient restHighLevelClient;
    private String hostname;
    private int port;

    public ElasticDaoImpl(String hostname, int port) {
        this.hostname = hostname;
        this.port = port;
    }

    private RestHighLevelClient getClient() {
        if (restHighLevelClient == null) {
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(hostname, port, "http")));
        }
        return restHighLevelClient;
    }

    @Override
    public List<SearchResult> search(String search) {
        return null;
    }

    @Override
    public void insert(Site site) {
        try (RestHighLevelClient client = getClient()) {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            builder.field("title", site.getTitle());
            builder.field("text", site.getPlainText());
            builder.field("keywords", site.getKeywords());
            builder.endObject();
            IndexRequest indexRequest = new IndexRequest("sites").id(site.getLink()).source(builder);
            IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
            System.out.println(indexResponse);
        } catch (IOException e) {
            logger.error(e);
        }
    }
}

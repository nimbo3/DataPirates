package in.nimbo;

import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.typesafe.config.Config;
import in.nimbo.model.SearchResult;
import org.apache.http.HttpHost;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ElasticSearch implements Searchable {
    private static Logger logger = Logger.getLogger(ElasticSearch.class);
    private final Config config;
    private Timer searchTimer;
    private RestHighLevelClient client;
    private String index;


    public ElasticSearch(Config config) {
        this.config = config;
        searchTimer = SharedMetricRegistries.getDefault().timer(config.getString("elastic.insertion.metric.name"));
        this.index = config.getString("elastic.index");
        client = new RestHighLevelClient(
                RestClient.builder(new HttpHost(
                        config.getString("elastic.hostname"),
                        config.getInt("elastic.port"))));
    }

    @Override
    public List<SearchResult> search(String input) {
        try (Timer.Context searchTime = searchTimer.time()) {
            SearchRequest searchRequest = new SearchRequest(config.getString("elastic.index"));
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.termQuery(config.getString("elastic.metadata.name"), input));
            searchSourceBuilder.query(QueryBuilders.termQuery(config.getString("elastic.keywords.name"), input));
            searchSourceBuilder.query(QueryBuilders.termQuery(config.getString("elastic.title.name"), input));
            searchSourceBuilder.query(QueryBuilders.termQuery(config.getString("elastic.text.name"), input));
            searchSourceBuilder.size(config.getInt("elastic.search.source.size"));
            searchRequest.source(searchSourceBuilder);
            SearchResponse searchResponse;
            try {
                searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
                List<SearchResult> searchResults = new ArrayList<>();
                for (SearchHit searchHit : searchResponse.getHits().getHits()) {
                    SearchResult searchResult = new SearchResult();
                    searchResult.setLink(searchHit.getId());
                    searchResult.setTitle(searchHit.getSourceAsMap().get(config.getString("elastic.title.name")).toString());
                    searchResults.add(searchResult);
                }
                return searchResults;
            } catch (IOException e) {
                logger.error(String.format("Elastic couldn't search [%s]", input), e);
                return new ArrayList<>();
            }
        }
    }
}

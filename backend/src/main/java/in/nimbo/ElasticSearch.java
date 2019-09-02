package in.nimbo;

import com.typesafe.config.Config;
import in.nimbo.model.ResultEntry;
import org.apache.http.HttpHost;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class ElasticSearch {
    private static Logger logger = Logger.getLogger(ElasticSearch.class);
    private RestHighLevelClient client;
    private String index;
    private int outputSize;
    private float textBoost;
    private float titleBoost;
    private float keywordsBoost;

    public ElasticSearch(Config config) {
        client = new RestHighLevelClient(
                RestClient.builder(new HttpHost(
                        config.getString("elastic.hostname"),
                        config.getInt("elastic.port"))));
        index = config.getString("elastic.index");
        outputSize = config.getInt("elastic.search.result.size");
        textBoost = (float) config.getDouble("elastic.search.text.boost");
        titleBoost = (float) config.getDouble("elastic.search.title.boost");
        keywordsBoost = (float) config.getDouble("elastic.search.keywords.boost");
    }

    public List<ResultEntry> search(String input) {
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("text", input));
        searchSourceBuilder.size(outputSize);
        searchRequest.source(searchSourceBuilder);
        return getResults(input, searchRequest);
    }

    public List<ResultEntry> multiMatchSearch(String input) {
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.multiMatchQuery(input)
                .field("title", titleBoost)
                .field("text", textBoost)
                .field("keywords", keywordsBoost));
        searchSourceBuilder.size(outputSize);
        searchRequest.source(searchSourceBuilder);
        return getResults(input, searchRequest);
    }

    public List<ResultEntry> fuzzySearch(String input) {
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.multiMatchQuery(input)
                .field("title", titleBoost)
                .field("text", textBoost)
                .field("keywords", keywordsBoost)
                .fuzziness(Fuzziness.AUTO));
        searchSourceBuilder.size(outputSize);
        searchRequest.source(searchSourceBuilder);
        return getResults(input, searchRequest);
    }

    private List<ResultEntry> getResults(String input, SearchRequest searchRequest) {
        SearchResponse searchResponse;
        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            List<ResultEntry> resultEntries = new ArrayList<>();
            for (SearchHit searchHit : searchResponse.getHits().getHits()) {
                ResultEntry resultEntry = new ResultEntry(searchHit.getId(),
                        searchHit.getSourceAsMap().get("title").toString(),
                        searchHit.getSourceAsMap().get("text").toString());
                resultEntries.add(resultEntry);
            }
            return resultEntries;
        } catch (IOException e) {
            logger.error(String.format("Elastic couldn't search [%s]", input), e);
            return new ArrayList<>();
        }
    }

    public List<String> autoComplete(String input, String lang) throws IOException {
        SearchRequest searchRequest = new SearchRequest(String.format("%s-%s", index, lang));
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.multiMatchQuery(input, "title.auto-complete"
                , "title.auto-complete._2gram", "title.auto-complete._3gram")
                .type(MultiMatchQueryBuilder.Type.BOOL_PREFIX)
        );
        searchSourceBuilder.size(20);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        Set<String> completionList = new LinkedHashSet<>();
        for (SearchHit searchHit : searchResponse.getHits().getHits()) {
            completionList.add(searchHit.getSourceAsMap().get("title").toString().toLowerCase());
            if (completionList.size() > 5)
                break;
        }
        return new ArrayList<>(completionList);
    }
}


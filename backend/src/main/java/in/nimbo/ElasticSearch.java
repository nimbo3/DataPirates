package in.nimbo;

import com.typesafe.config.Config;
import in.nimbo.model.Query;
import in.nimbo.model.ResultEntry;
import org.apache.http.HttpHost;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.*;

public class ElasticSearch {
    private static Logger logger = Logger.getLogger(ElasticSearch.class);
    private RestHighLevelClient client;
    private String indexPrefix = "sites-";
    private String index = "sites-en";
    private int outputSize;
    private float textBoost;
    private float titleBoost;
    private float keywordsBoost;

    public ElasticSearch(Config config) {
        client = new RestHighLevelClient(
                RestClient.builder(new HttpHost(
                        config.getString("elastic.hostname"),
                        config.getInt("elastic.port"))));
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

    public List<ResultEntry> search(String queryString, String language) {
        Query query = Query.build(queryString);
        SearchRequest searchRequest = new SearchRequest(indexPrefix + language);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.simpleQueryStringQuery(query.getText())
                        .quoteFieldSuffix(".raw")
                        .field("title", titleBoost)
                        .field("text", textBoost)
                        .field("keyword", keywordsBoost));
        if (query.getDomain() != null)
            queryBuilder.filter(QueryBuilders.termQuery("domain", query.getDomain()));
        query.getExclude().forEach(exclude -> queryBuilder.mustNot(QueryBuilders.termQuery("text", exclude)));
        searchSourceBuilder.query(queryBuilder);
        searchSourceBuilder.size(outputSize);
        searchRequest.source(searchSourceBuilder);
        return getResults(queryString, searchRequest);
    }

    private List<ResultEntry> getResults(String input, SearchRequest searchRequest) {
        SearchResponse searchResponse;
        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            List<ResultEntry> resultEntries = new ArrayList<>();
            for (SearchHit searchHit : searchResponse.getHits().getHits()) {
                Map<String, Object> hitMap = searchHit.getSourceAsMap();
                ResultEntry resultEntry = new ResultEntry(hitMap.get("title").toString(),
                        hitMap.get("link").toString(),
                        hitMap.get("text").toString());
                if (hitMap.get("page-rank") != null)
                    resultEntry.setPageRank(Double.parseDouble(hitMap.get("page-rank").toString()));
                else
                    resultEntry.setPageRank(1);
                if (hitMap.get("tags") != null)
                    resultEntry.setTags(Arrays.asList(hitMap.get("tags").toString().split(",")));
                resultEntries.add(resultEntry);
            }
            return resultEntries;
        } catch (IOException e) {
            logger.error(String.format("Elastic couldn't search [%s]", input), e);
            return new ArrayList<>();
        }
    }
}


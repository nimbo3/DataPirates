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
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;

import java.io.IOException;
import java.util.*;

public class ElasticSearch {
    private static Logger logger = Logger.getLogger(ElasticSearch.class);
    private RestHighLevelClient client;
    private String indexPrefix = "sites-";
    private String index = "sites-en";
    private int outputSize;
    private int summaryMaxLength;
    private float textBoost;
    private float titleBoost;
    private float keywordsBoost;

    public ElasticSearch(Config config) {
        client = new RestHighLevelClient(
                RestClient.builder(new HttpHost(
                        config.getString("elastic.hostname"),
                        config.getInt("elastic.port"))));
        outputSize = config.getInt("elastic.search.result.size");
        summaryMaxLength = config.getInt("elastic.search.result.summary.length.max");
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
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.simpleQueryStringQuery(query.getText())
                        .quoteFieldSuffix(".raw")
                        .field("title", titleBoost)
                        .field("text", textBoost)
                        .field("keyword", keywordsBoost));
        if (query.getDomain() != null)
            boolQueryBuilder.filter(QueryBuilders.termQuery("domain", query.getDomain()));
        query.getExclude().forEach(exclude -> boolQueryBuilder.mustNot(QueryBuilders.termQuery("text", exclude)));
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        HighlightBuilder.Field highlightTitle =
                new HighlightBuilder.Field("text");
        highlightTitle.highlighterType("unified");
        highlightBuilder.field(highlightTitle);
        searchSourceBuilder.query(boolQueryBuilder);
        searchSourceBuilder.highlighter(highlightBuilder);
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
                ResultEntry resultEntry = convertToResultEntry(searchHit);
                resultEntries.add(resultEntry);
            }
            return resultEntries;
        } catch (IOException e) {
            logger.error(String.format("Elastic couldn't search [%s]", input), e);
            return new ArrayList<>();
        }
    }

    private ResultEntry convertToResultEntry(SearchHit searchHit) {
        Map<String, Object> hitMap = searchHit.getSourceAsMap();
        String summary = extractSummary(searchHit, hitMap);
        ResultEntry resultEntry = new ResultEntry(hitMap.get("title").toString(),
                hitMap.get("link").toString(),
                summary);
        if (hitMap.get("page-rank") != null)
            resultEntry.setPageRank(Double.parseDouble(hitMap.get("page-rank").toString()));
        else
            resultEntry.setPageRank(1);
        if (hitMap.get("tags") != null)
            resultEntry.setTags(Arrays.asList(hitMap.get("tags").toString().split(",")));
        return resultEntry;
    }

    private String extractSummary(SearchHit searchHit, Map<String, Object> hitMap) {
        String summary = "";
        try {
            Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
            HighlightField highlight = highlightFields.get("text");
            Text[] fragments = highlight.fragments();
            for (Text fragment : fragments)
                if (summary.length() < summaryMaxLength)
                    summary = summary + "..." + fragment;
        } catch (NullPointerException e) {
            summary = hitMap.get("text").toString();
            if (summary.length() > summaryMaxLength)
                summary = summary.substring(0, summaryMaxLength) + "...";
        }
        return summary;
    }
}


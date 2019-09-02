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
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.common.text.Text;
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
        query.getExclude().forEach(exclude ->
                boolQueryBuilder.mustNot(QueryBuilders.termQuery("text", exclude)));
        searchSourceBuilder.query(boolQueryBuilder);
        searchSourceBuilder.highlighter(getHighlightBuilder("text"));
        searchSourceBuilder.size(outputSize);
        searchRequest.source(searchSourceBuilder);
        return getResults(queryString, searchRequest);
    }

    private HighlightBuilder getHighlightBuilder(String fieldName) {
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        HighlightBuilder.Field highlightText =
                new HighlightBuilder.Field(fieldName);
        highlightText.preTags("<b>");
        highlightText.postTags("</b>");
        highlightText.highlighterType("unified");
        highlightBuilder.field(highlightText);
        return highlightBuilder;
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

    public List<String> autoComplete(String input, String lang) throws IOException {
        SearchRequest searchRequest = new SearchRequest(indexPrefix + lang);
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
  
    private ResultEntry convertToResultEntry(SearchHit searchHit) {
        Map<String, Object> hitMap = searchHit.getSourceAsMap();
        String summary = extractSummary(searchHit, hitMap);
        ResultEntry resultEntry = new ResultEntry(hitMap.get("title").toString(),
                hitMap.get("link").toString(),
                hitMap.get("text").toString(),
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


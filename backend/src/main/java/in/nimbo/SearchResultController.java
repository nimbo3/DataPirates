package in.nimbo;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import in.nimbo.model.*;
import in.nimbo.model.exceptions.HbaseException;
import org.apache.hadoop.hbase.client.Result;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

@RestController
public class SearchResultController {

    private static Hbase hbase;

    public static void setHbase(Hbase hbase) {
        SearchResultController.hbase = hbase;
    }

    @CrossOrigin
    @GetMapping("/search")
    public List<ResultEntry> greeting(@RequestParam(value = "input") String input, @RequestParam(value = "type", defaultValue = "2") int type) {
        Config config = ConfigFactory.load("config");
        ElasticSearch elasticSearch = new ElasticSearch(config);
        if (type == 1)
            return elasticSearch.search(input);
        else if (type == 2)
            return elasticSearch.multiMatchSearch(input);
        else if (type == 3)
            return elasticSearch.fuzzySearch(input);
        throw new IllegalArgumentException();
    }

    @CrossOrigin
    @GetMapping("/web-graph/single-domain")
    public WebGraphResult singleDomainGraph(@RequestParam(value = "domain") String domain) {
        try {
            Result result = hbase.get(domain);
            Set<Edge> edges = new LinkedHashSet<>();
            Set<Vertex> verteces = new LinkedHashSet<>();
            WebGraphDomains.extractHbaseResultToGraph(edges, verteces, result);
            return new WebGraphResult(new ArrayList<>(verteces), new ArrayList<>(edges));
        } catch (HbaseException e) {
            return null;
        }
    }

    @CrossOrigin
    @GetMapping("/web-graph/top-domains")
    public WebGraphResult topDomainsGraph() {
        return WebGraphDomains.getWebGraphResult();
    }
}


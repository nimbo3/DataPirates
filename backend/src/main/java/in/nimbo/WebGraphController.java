package in.nimbo;

import in.nimbo.model.Edge;
import in.nimbo.model.Hbase;
import in.nimbo.model.Vertex;
import in.nimbo.model.WebGraphResult;
import in.nimbo.model.exceptions.HbaseException;
import org.apache.hadoop.hbase.client.Result;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Set;

@RestController
public class WebGraphController {

    private static Hbase hbase;

    public static void setHbase(Hbase hbase) {
        WebGraphController.hbase = hbase;
    }

    @CrossOrigin
    @GetMapping("/web-graph/single-domain")
    public WebGraphResult singleDomainGraph(@RequestParam(value = "domain") String domain) {
        try {
            Result result = hbase.get(domain);
            Set<Edge> edges = new LinkedHashSet<>();
            Set<Vertex> vertices = new LinkedHashSet<>();
            WebGraphDomains.extractHbaseResultToGraph(edges, vertices, result);
            return new WebGraphResult(new ArrayList<>(vertices), new ArrayList<>(edges));
        } catch (HbaseException e) {
            return null;
        }
    }

    @CrossOrigin
    @GetMapping("/web-graph/top-domains")
    public WebGraphResult topDomainsGraph() {
        return WebGraphDomains.getInstance().getWebGraphResult();
    }
}

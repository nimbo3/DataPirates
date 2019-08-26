package in.nimbo;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import in.nimbo.model.*;
import in.nimbo.model.exceptions.HbaseException;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;

@RestController
public class SearchResultController {

    private static Hbase hbase;

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
            if (result.listCells() != null) {
                Set<Edge> edges = new LinkedHashSet<>();
                Set<Vertex> verteces = new LinkedHashSet<>();
                verteces.add(new Vertex(Bytes.toString(result.getRow()), "#17a2b8"));
                result.listCells().forEach(cell -> {
                    String row = Bytes.toString(CellUtil.cloneRow(cell));
                    String family = Bytes.toString(CellUtil.cloneFamily(cell));
                    String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                    Integer value = Bytes.toInt(CellUtil.cloneValue(cell));
                    verteces.add(new Vertex(qualifier));
                    Edge edge;
                    if (!row.equals(qualifier)) {
                        if (family.equals("i")) {
                            edge = new Edge(qualifier, row, value);
                        } else {
                            edge = new Edge(row, qualifier, value);
                        }
                        edges.add(edge);
                    }
                });
                return new WebGraphResult(new ArrayList<>(verteces), new ArrayList<>(edges));
            } else {
                return null;
            }
        } catch (HbaseException e) {
            return null;
        }
    }


    public static void setHbase(Hbase hbase) {
        SearchResultController.hbase = hbase;
    }
}


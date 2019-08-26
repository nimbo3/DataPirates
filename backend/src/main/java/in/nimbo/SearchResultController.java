package in.nimbo;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import in.nimbo.model.Hbase;
import in.nimbo.model.Pair;
import in.nimbo.model.ResultEntry;
import in.nimbo.model.exceptions.HbaseException;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
    public List<String> singleDomainGraph(@RequestParam(value = "google.com") String domain) {
        try {
            Result result = hbase.get(domain);
            Map<String, Integer> inputDomains = new LinkedHashMap<>();
            Map<String, Integer> outputDomains = new LinkedHashMap<>();
            result.listCells().forEach(cell -> {
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                Integer value = Bytes.toInt(CellUtil.cloneValue(cell));
                if (family.equals("i")) {
                    inputDomains.put(qualifier, value);
                } else {
                    outputDomains.put(qualifier, value);
                }
            });

            return null;
        } catch (HbaseException e) {
            return new ArrayList<>();
        }
    }


    public static void setHbase(Hbase hbase) {
        SearchResultController.hbase = hbase;
    }
}


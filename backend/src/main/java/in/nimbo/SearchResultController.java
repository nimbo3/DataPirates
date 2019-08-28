package in.nimbo;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.jmx.JmxReporter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import in.nimbo.model.ResultEntry;
import org.springframework.web.bind.annotation.*;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

@RestController
public class SearchResultController {

    @CrossOrigin
    @GetMapping("/search")
    public List<ResultEntry> greeting(@RequestParam(value = "query") String query, @RequestParam(value = "lang") String lang) {
        Config config = ConfigFactory.load("config");
        ElasticSearch elasticSearch = new ElasticSearch(config);
        return Arrays.asList(new ResultEntry("title", "example.com", "hello!!", 2, Arrays.asList("12", "13")));
    }
}


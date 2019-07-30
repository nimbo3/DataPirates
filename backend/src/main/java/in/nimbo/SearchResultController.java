package in.nimbo;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.jmx.JmxReporter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import in.nimbo.model.SearchResult;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
public class SearchResultController {

    @GetMapping("/search")
    public List<SearchResult> greeting(@RequestParam(value="input", defaultValue="") String input) {
        Config config = ConfigFactory.load("config");
        SharedMetricRegistries.setDefault(config.getString("metric.registry.name"));
        MetricRegistry metricRegistry = SharedMetricRegistries.getDefault();
        JmxReporter jmxReporter = JmxReporter.forRegistry(metricRegistry).inDomain(config.getString("metric.domain.name")).build();
        jmxReporter.start();
        ElasticSearch elasticSearch = new ElasticSearch(config);
        return elasticSearch.search(input);
    }
}

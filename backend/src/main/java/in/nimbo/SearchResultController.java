package in.nimbo;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import in.nimbo.model.SearchResult;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

@RestController
public class SearchResultController {

    @GetMapping("/search")
    public List<SearchResult> greeting(@RequestParam(value="input", defaultValue="") String input) {
        Config config = ConfigFactory.load("config");
        ElasticSearch elasticSearch = new ElasticSearch(config);
        return elasticSearch.search(input);
    }
}

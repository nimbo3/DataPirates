package in.nimbo;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import in.nimbo.model.ResultEntry;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class SearchResultController {
    Config config = ConfigFactory.load("config");
    ElasticSearch elasticSearch = new ElasticSearch(config);

    @CrossOrigin
    @GetMapping("/search")
    public List<ResultEntry> greeting(@RequestParam(value = "query") String query, @RequestParam(value = "lang") String lang) {
        return elasticSearch.search(query, lang);
    }
}


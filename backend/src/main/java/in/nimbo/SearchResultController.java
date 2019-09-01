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
}


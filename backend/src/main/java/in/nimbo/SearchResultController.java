package in.nimbo;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import in.nimbo.model.ResultEntry;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

@RestController
public class SearchResultController {

    private static Config config = ConfigFactory.load("config");

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
    @GetMapping("/api/autocomplete")
    public List<String> autoComplete(@RequestParam(value = "input") String input, @RequestParam(value = "lang") String lang) {
        ElasticSearch elasticSearch = new ElasticSearch(config);
        try {
            List<String> autoComplete = elasticSearch.autoComplete(input, lang.toLowerCase());
            return autoComplete;
        } catch (IOException e) {
            e.printStackTrace();
            return Arrays.asList(
                    "salam",
                    "khoobi",
                    "chetori"
            );
        }
    }
}


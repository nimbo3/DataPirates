package in.nimbo;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import in.nimbo.model.ResultEntry;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.ArrayList;
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

    @CrossOrigin
    @GetMapping("/api/autocomplete")
    public List<String> autoComplete(@RequestParam(value = "input") String input, @RequestParam(value = "lang") String lang) {
        try {
            if (input.contains("domain:") || input.contains("-") || input.contains("\""))
                return new ArrayList<>();
            else
                return elasticSearch.autoComplete(input, lang.toLowerCase());
        } catch (IOException e) {
            e.printStackTrace();
            return new ArrayList<>();
        }
    }
}


package in.nimbo;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import in.nimbo.model.ResultEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.*;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.BinaryOperator;

@RestController
public class SearchResultController {
    Logger logger = LoggerFactory.getLogger(SearchResultController.class);
    Config config = ConfigFactory.load("config");
    ElasticSearch elasticSearch = new ElasticSearch(config);

    @CrossOrigin
    @GetMapping("/search")
    public List<List<ResultEntry>> greeting(@RequestParam(value = "query") String query, @RequestParam(value = "lang") String lang) {
        final List<ResultEntry> searchResult = elasticSearch.search(query, lang);
        try {
            URL url = new URL("http://slave2:5000/cluster");
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("POST");
            con.setRequestProperty("Content-Type", "application/json; utf-8");
            con.setRequestProperty("Accept", "application/json");
            con.setDoOutput(true);
            try (OutputStream out = new ObjectOutputStream(con.getOutputStream())) {
                final String json = new Gson().toJson(searchResult);
                System.out.println("=" + json);
                byte[] input = json.getBytes("utf-8");
                OutputStream os = con.getOutputStream();
                OutputStreamWriter outputStreamWriter = new OutputStreamWriter(os, "UTF-8");
                outputStreamWriter.write(json);
//                outputStreamWriter.writeObject(json);
                outputStreamWriter.flush();
                int responseCode = con.getResponseCode();
                if(responseCode == 200){
                    ObjectInputStream inputStream = new ObjectInputStream(con.getInputStream());
                    final String jsonResponse = (String)inputStream.readObject();
                    Type listType = new TypeToken<List<ResultEntry>>(){}.getType();
                    List<ResultEntry> responseList = new Gson().fromJson(jsonResponse, listType);
                    Map<Integer, List<ResultEntry>> map = new HashMap<>();
                    for (ResultEntry resultEntry : responseList) {
                        final int clusterId = resultEntry.getClusterId();
                        if(!map.containsKey(clusterId)){
                            List<ResultEntry> list = new ArrayList<>();
                            list.add(resultEntry);
                            map.put(clusterId, list);
                        }else{
                            final List<ResultEntry> list = map.get(clusterId);
                            list.add(resultEntry);
                            map.put(clusterId, list);
                        }
                    }
                    return (List<List<ResultEntry>>)map.values();
                }else{
                    logger.error("response code is: " + responseCode);
                }
            } catch (IOException e) {
                logger.error("Can't open stream to send json file!", e);
            } catch (ClassNotFoundException e) {
                logger.error("can't get object from response of post request", e);
            }
        } catch (ProtocolException e) {
            logger.error("There is no protocol for your HTTP request", e);
        } catch (MalformedURLException e) {
            logger.error("The HTTP request is not correct!", e);
        } catch (IOException e) {
            logger.error("Can't open connection for the requested HTTP url!", e);
        }
        return new ArrayList<>();
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


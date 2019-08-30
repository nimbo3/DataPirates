package in.nimbo;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class TopSites {

    private static Logger logger = LoggerFactory.getLogger(TopSites.class);

    public static void main(String[] args) {
        try {
            updateListOfTopDomains();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public TopSites() {
    }

    public static void updateListOfTopDomains() throws IOException {
        String url = "https://www.alexa.com/topsites";
        Document document = Jsoup.connect(url)
                .followRedirects(true)
                .timeout(20000)
                .get();

        try (FileWriter fileWriter = new FileWriter("top-domains.txt")) {
            for (Element urlContainer : document.getElementsByClass("DescriptionCell")) {
                fileWriter.append(String.format("%s\n",
                        urlContainer.select("a").get(0).text().toLowerCase()));
            }
            fileWriter.flush();
        }
    }
}

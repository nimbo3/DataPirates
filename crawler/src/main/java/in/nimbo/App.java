package in.nimbo;

import in.nimbo.util.cacheManager.CaffeineVistedSiteCache;
import org.apache.http.client.RedirectException;

import java.io.IOException;

public class App {
    public static void main(String[] args) {
        FetcherImpl fetcher = new FetcherImpl();
        int constant = 1000;
        CrawlerThread[] crawlerThreads = new CrawlerThread[constant];
        for (int i = 0; i < constant; i++) {
            crawlerThreads[i] = new CrawlerThread(fetcher);
        }
        for (int i = 0; i < constant; i++) {
            crawlerThreads[i].start();
        }
    }
}

class CrawlerThread extends Thread {
    FetcherImpl fetcher;

    public CrawlerThread(FetcherImpl fetcher) {
        this.fetcher = fetcher;
    }

    @Override
    public void run() {
        boolean quit = false;
        while (!quit) {
            // TODO: 7/22/19 get url from Kafka
            String url = "https://tabnak.ir";
            // TODO: 7/22/19 check if url has been checked recently
            try {
                fetcher.fetch(url);
            } catch (IOException e) {
                // TODO: 7/22/19 do something in here
                e.printStackTrace();
            } catch (RedirectException e) {
                // TODO: 7/23/19 do another thing in here
            }
            // TODO: 7/22/19 parse and insert into databases
        }
    }
}

package in.nimbo;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import in.nimbo.database.dao.HbaseSiteDaoImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.http.client.RedirectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class App {
    private static final Logger logger = LoggerFactory.getLogger(App.class);
    public static void main(String[] args) {
        Configuration hbaseConfig = HBaseConfiguration.create();
        String path = HBaseConfiguration.class.getClassLoader().getResource("xml/hbase-site.xml").getPath();
        hbaseConfig.addResource(new Path(path));
        Config config = ConfigFactory.load("config");
        HbaseSiteDaoImpl hbaseSiteDaoImpl;
        try {
            hbaseSiteDaoImpl = new HbaseSiteDaoImpl(hbaseConfig, config);
        } catch (IOException e) {
            logger.error("can't connect to Hbase", e);
            //TODO do sth after failing connection to Hbase
        }
//        FetcherImpl fetcher = new FetcherImpl();
//        int constant = 1000;
//        CrawlerThread[] crawlerThreads = new CrawlerThread[constant];
//        for (int i = 0; i < constant; i++) {
//            crawlerThreads[i] = new CrawlerThread(fetcher);
//        }
//        for (int i = 0; i < constant; i++) {
//            crawlerThreads[i].start();
//        }
    }
}

class CrawlerThread extends Thread {
    FetcherImpl fetcher;

    public CrawlerThread(FetcherImpl fetcher) {
        this.fetcher = fetcher;
    }

    public String getDomain(String url){
        Pattern regex = Pattern.compile("^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\\?([^#]*))?(#(.*))?");
        Matcher matcher = regex.matcher(url);
        if (matcher.find()){
            return matcher.group(4);
        }
        return url;
    }
    @Override
    public void run() {
        boolean quit = false;
        while (!quit) {
            // TODO: 7/22/19 get url from Kafka
            String url = "https://tabnak.ir";
            String domain = getDomain(url);
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

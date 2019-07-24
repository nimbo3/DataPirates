package in.nimbo;

import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import in.nimbo.database.ElasticDaoImpl;
import in.nimbo.database.dao.SiteDao;
import in.nimbo.model.Site;
import in.nimbo.parser.Parser;
import in.nimbo.util.LinkConsumer;
import in.nimbo.util.UnusableSiteDetector;
import in.nimbo.util.VisitedLinksCache;
import in.nimbo.util.cacheManager.CaffeineVistedDomainCache;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class App {

    public static String getDomain(String url) {
        Pattern regex = Pattern.compile("^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\\?([^#]*))?(#(.*))?");
        Matcher matcher = regex.matcher(url);
        if (matcher.find()) {
            return matcher.group(4);
        }
        return url;
    }

    public static void main(String[] args) {
        try {
            DetectorFactory.loadProfile(Paths.get("./profiles").toAbsolutePath().toFile());
        } catch (LangDetectException e) {
            e.printStackTrace();
        }

        try {
            TrustManager[] trustAllCerts = new TrustManager[]{new X509TrustManager() {
                public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                    return null;
                }

                public void checkClientTrusted(X509Certificate[] certs, String authType) {
                }

                public void checkServerTrusted(X509Certificate[] certs, String authType) {
                }
            }};

            SSLContext sc = null;

            sc = SSLContext.getInstance("SSL");

            sc.init(null, trustAllCerts, new java.security.SecureRandom());
            HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
        } catch (NoSuchAlgorithmException | KeyManagementException e) {
            e.printStackTrace();
        }

        Config config = ConfigFactory.load("config");
        FetcherImpl fetcher = new FetcherImpl();
        int threads = config.getInt("fetcher.threads");
        CrawlerThread[] crawlerThreads = new CrawlerThread[threads];
        VisitedLinksCache visitedUrlsCache = new VisitedLinksCache() {
            Map<String, Integer> visitedUrls = new ConcurrentHashMap<>();

            @Override
            public void put(String normalizedUrl) {
                visitedUrls.put(normalizedUrl, 0);
            }

            @Override
            public boolean hasVisited(String normalizedUrl) {
                return visitedUrls.keySet().contains(normalizedUrl);
            }
        };
        CaffeineVistedDomainCache vistedDomainCache = new CaffeineVistedDomainCache(config);
        ElasticDaoImpl elasticDao = new ElasticDaoImpl("slave1", 9200);
        Properties kafkaConsumerProperties = new Properties();
        Properties kafkaProducerProperties = new Properties();
        try {
            kafkaConsumerProperties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("KafkaConsumer.properties"));
            kafkaProducerProperties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("KafkaProducer.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaConsumerProperties);
        LinkConsumer linkConsumer = new LinkConsumer(consumer, config);
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProducerProperties);
        linkConsumer.start();

        for (int i = 0; i < threads; i++) {
            crawlerThreads[i] = new CrawlerThread(fetcher,
                    vistedDomainCache,
                    visitedUrlsCache,
                    linkConsumer,
                    kafkaProducer,
                    elasticDao);
        }
        for (int i = 0; i < threads; i++) {
            crawlerThreads[i].start();
        }
    }
}

class CrawlerThread extends Thread {
    private static Logger logger = Logger.getLogger(CrawlerThread.class);
    private FetcherImpl fetcher;
    private VisitedLinksCache visitedDomainsCache;
    private VisitedLinksCache visitedUrlsCache;
    private LinkConsumer linkConsumer;
    private KafkaProducer kafkaProducer;
    private SiteDao database;
    private UnusableSiteDetector unusableSiteDetector;

    public CrawlerThread(FetcherImpl fetcher,
                         VisitedLinksCache visitedDomainsCache, VisitedLinksCache visitedUrlsCache,
                         LinkConsumer linkConsumer, KafkaProducer kafkaProducer, SiteDao database) {
        this.fetcher = fetcher;
        this.visitedDomainsCache = visitedDomainsCache;
        this.database = database;
        this.linkConsumer = linkConsumer;
        this.kafkaProducer = kafkaProducer;
        this.visitedUrlsCache = visitedUrlsCache;
    }

    @Override
    public void run() {
        while (!interrupted()) {
            String url = null;
            try {
                url = linkConsumer.pop();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info(String.format("New link (%s) poped from queue", url));
            if (!visitedDomainsCache.hasVisited(App.getDomain(url)) &&
                    !visitedUrlsCache.hasVisited(url)) {
                try {
                    fetcher.fetch(url);
                    if (fetcher.isContentTypeTextHtml()) {
                        Parser parser = new Parser(url, fetcher.getRawHtmlDocument());
                        Site site = parser.parse();
                        if (new UnusableSiteDetector(site.getPlainText()).hasAcceptableLanguage()) {
                            visitedDomainsCache.put(App.getDomain(url));
                            //Todo : Check In Travis And Then Put
                            site.getAnchors().keySet().forEach(link -> kafkaProducer.send(new ProducerRecord("links", link)));
                            visitedUrlsCache.put(url);
                            database.insert(site);
                            System.out.println(site.getTitle() + " : " + site.getLink());
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    logger.error(e);
                }
            } else {
                kafkaProducer.send(new ProducerRecord("links", url));
                logger.info(String.format("New link (%s) pushed to queue", url));
            }
        }
    }
}

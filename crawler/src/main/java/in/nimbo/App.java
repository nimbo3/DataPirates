package in.nimbo;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import in.nimbo.database.dao.ElasticSiteDaoImpl;
import in.nimbo.database.dao.HbaseSiteDaoImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import in.nimbo.util.LinkConsumer;
import in.nimbo.util.VisitedLinksCache;
import in.nimbo.util.cacheManager.CaffeineVistedDomainCache;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class App {
    private static Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        try {
            DetectorFactory.loadProfile(Paths.get("./profiles").toAbsolutePath().toFile());
        } catch (LangDetectException e) {
            logger.error("./profiles can't be loaded, lang detection not started", e);
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
            logger.error("SSl can't be established", e);
        }

        Config config = ConfigFactory.load("config");
        Configuration hbaseConfig = HBaseConfiguration.create();
        HbaseSiteDaoImpl hbaseDao = new HbaseSiteDaoImpl(hbaseConfig, config);

        int numberOfFetcherThreads = config.getInt("num.of.fetcher.threads");
        int elasticPort = config.getInt("elastic.port");
        String elasticHostname = config.getString("elastic.hostname");

        FetcherImpl fetcher = new FetcherImpl(config);
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
        ElasticSiteDaoImpl elasticDao = new ElasticSiteDaoImpl(elasticHostname, elasticPort);
        Properties kafkaConsumerProperties = new Properties();
        Properties kafkaProducerProperties = new Properties();
        try {
            kafkaConsumerProperties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("KafkaConsumer.properties"));
            kafkaProducerProperties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("KafkaProducer.properties"));
        } catch (IOException e) {
            logger.error("kafka properties can't be loaded", e);
        }
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaConsumerProperties);
        LinkConsumer linkConsumer = new LinkConsumer(consumer, config);
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProducerProperties);
        linkConsumer.start();


        CrawlerThread[] crawlerThreads = new CrawlerThread[numberOfFetcherThreads];
        for (int i = 0; i < numberOfFetcherThreads; i++) {
            crawlerThreads[i] = new CrawlerThread(fetcher,
                    vistedDomainCache,
                    visitedUrlsCache,
                    linkConsumer,
                    kafkaProducer,
                    elasticDao,
                    hbaseDao);
        }
        for (int i = 0; i < numberOfFetcherThreads; i++) {
            crawlerThreads[i].start();
        }
    }
}


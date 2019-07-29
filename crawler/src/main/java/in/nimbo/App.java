package in.nimbo;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.codahale.metrics.jmx.JmxReporter;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import in.nimbo.dao.ElasticSiteDaoImpl;
import in.nimbo.dao.HbaseSiteDaoImpl;
import in.nimbo.exception.HbaseSiteDaoException;
import in.nimbo.util.LinkConsumer;
import in.nimbo.util.LinkProducer;
import in.nimbo.util.RedisVisitedLinksCache;
import in.nimbo.util.VisitedLinksCache;
import in.nimbo.util.cacheManager.CaffeineVistedDomainCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;

public class App {
    private static Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        Config config = ConfigFactory.load("config");
        SharedMetricRegistries.setDefault(config.getString("metric.registery.name"));
        MetricRegistry metricRegistry = SharedMetricRegistries.getDefault();
        JmxReporter jmxReporter = JmxReporter.forRegistry(metricRegistry).inDomain("crawler").build();
        jmxReporter.start();
        Timer appInitializingMetric = metricRegistry.timer("app initializing");
        try (Timer.Context appInitializingTimer = appInitializingMetric.time()) {
            try {
                DetectorFactory.loadProfile(config.getString("langDetector.profile"));
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
                logger.error("SSl can't be es   tablished", e);
            }

            Configuration hbaseConfig = HBaseConfiguration.create();
            HbaseSiteDaoImpl hbaseDao = new HbaseSiteDaoImpl(hbaseConfig, config);

            int numberOfFetcherThreads = config.getInt("num.of.fetcher.threads");

            FetcherImpl fetcher = new FetcherImpl(config);
            VisitedLinksCache visitedUrlsCache = new RedisVisitedLinksCache(config);
            CaffeineVistedDomainCache vistedDomainCache = new CaffeineVistedDomainCache(config);
            ElasticSiteDaoImpl elasticDao = new ElasticSiteDaoImpl(config);

            LinkConsumer linkConsumer = new LinkConsumer(config);
            linkConsumer.start();
            LinkProducer linkProducer = new LinkProducer(config);

            CrawlerThread[] crawlerThreads = new CrawlerThread[numberOfFetcherThreads];
            for (int i = 0; i < numberOfFetcherThreads; i++) {
                crawlerThreads[i] = new CrawlerThread(fetcher,
                        vistedDomainCache,
                        visitedUrlsCache,
                        linkConsumer,
                        linkProducer,
                        elasticDao, hbaseDao);
            }
            for (int i = 0; i < numberOfFetcherThreads; i++) {
                crawlerThreads[i].start();
            }
        } catch (HbaseSiteDaoException e) {
            logger.error(e.getMessage(), e);
        }
    }
}


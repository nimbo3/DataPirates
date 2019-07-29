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
import java.io.Closeable;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.LinkedList;
import java.util.List;

public class App {
    private static final Config config = ConfigFactory.load("config");
    private static Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        List<Closeable> closeables = new LinkedList<>();
        ShutdownHook shutdownHook = new ShutdownHook(closeables);
        Runtime.getRuntime().addShutdownHook(shutdownHook);
        SharedMetricRegistries.setDefault(config.getString("metric.registry.name"));
        MetricRegistry metricRegistry = SharedMetricRegistries.getDefault();
        JmxReporter jmxReporter = JmxReporter.forRegistry(metricRegistry).inDomain(config.getString("metric.domain.name")).build();
        jmxReporter.start();
        Timer appInitializingMetric = metricRegistry.timer(config.getString("metric.registry.timer.name"));
        try (Timer.Context appInitializingTimer = appInitializingMetric.time()) {
            try {
                DetectorFactory.loadProfile(config.getString("langDetect.profile.dir"));
            } catch (LangDetectException e) {
                logger.error("langDetector profile can't be loaded, lang detection not started", e);
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
            closeables.add(hbaseDao);
            int numberOfFetcherThreads = config.getInt("num.of.fetcher.threads");
            FetcherImpl fetcher = new FetcherImpl(config);
            closeables.add(fetcher);
            VisitedLinksCache visitedUrlsCache = new RedisVisitedLinksCache(config);
            closeables.add(visitedUrlsCache);
            CaffeineVistedDomainCache vistedDomainCache = new CaffeineVistedDomainCache(config);
            closeables.add(vistedDomainCache);
            ElasticSiteDaoImpl elasticDao = new ElasticSiteDaoImpl(config);
            closeables.add(elasticDao);
            LinkConsumer linkConsumer = new LinkConsumer(config);
            //TODO linkConsumer shutdown hook should be added
            linkConsumer.start();
            LinkProducer linkProducer = new LinkProducer(config);
            //TODO linkProducer shutdown hook should be added

            CrawlerThread[] crawlerThreads = new CrawlerThread[numberOfFetcherThreads];
            for (int i = 0; i < numberOfFetcherThreads; i++) {
                crawlerThreads[i] = new CrawlerThread(fetcher,
                        vistedDomainCache,
                        visitedUrlsCache,
                        linkConsumer,
                        linkProducer,
                        elasticDao,
                        hbaseDao);
                closeables.add(crawlerThreads[i]);
                crawlerThreads[i].start();
            }
        } catch (HbaseSiteDaoException e) {
            logger.error(e.getMessage(), e);
        }
    }
}

class ShutdownHook extends Thread {
    private static Logger logger = LoggerFactory.getLogger(ShutdownHook.class);
    private List<Closeable> closeables;

    public ShutdownHook(List<Closeable> closeables) {
        this.closeables = closeables;
    }

    public void run() {
        logger.info("Shutdown hook thread initiated.");
        for (Closeable closeable : closeables) {
            try {
                closeable.close();
            } catch (IOException e) {
                logger.error("Shutdown hook can't close object with name: " + closeable.getClass().getSimpleName(), e);
            }
        }
    }
}


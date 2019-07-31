package in.nimbo;

import com.codahale.metrics.Gauge;
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
import in.nimbo.model.Pair;
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
import java.io.File;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.concurrent.LinkedBlockingQueue;

public class App {
    private static Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        SharedMetricRegistries.setDefault("data-pirates-crawler");
        MetricRegistry metricRegistry = SharedMetricRegistries.getDefault();
        JmxReporter jmxReporter = JmxReporter.forRegistry(metricRegistry).inDomain("crawler").build();
        jmxReporter.start();
        Timer appInitializingMetric = metricRegistry.timer("app initializing");
        try (Timer.Context appInitializingTimer = appInitializingMetric.time()) {
            try {
                DetectorFactory.loadProfile("profiles");
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

            Config outConfig = ConfigFactory.parseFile(new File("config.properties"));
            Config inConfig = ConfigFactory.load("config");
            Config config = ConfigFactory.load(outConfig).withFallback(inConfig);
            Configuration hbaseConfig = HBaseConfiguration.create();
            HbaseSiteDaoImpl hbaseDao = new HbaseSiteDaoImpl(hbaseConfig, config);

            int numberOfFetcherThreads = config.getInt("num.of.fetcher.threads");
            int numberOfProcessorThreads = config.getInt("num.of.processor.threads");

            VisitedLinksCache visitedUrlsCache = new RedisVisitedLinksCache(config);
            CaffeineVistedDomainCache vistedDomainCache = new CaffeineVistedDomainCache(config);
            ElasticSiteDaoImpl elasticDao = new ElasticSiteDaoImpl(config);

            LinkConsumer linkConsumer = new LinkConsumer(config);
            linkConsumer.start();
            LinkProducer linkProducer = new LinkProducer(config);
            LinkedBlockingQueue<Pair<String, String>> linkPairHtmlQueue = new LinkedBlockingQueue<>();
            SharedMetricRegistries.getDefault().register(
                    MetricRegistry.name(FetcherThread.class, "linkPairHtmlQueue", "size"),
                    (Gauge<Integer>) linkPairHtmlQueue::size);
            FetcherImpl fetcher = new FetcherImpl(config);
            JsoupFetcher jsoupFetcher = new JsoupFetcher();

            FetcherThread[] fetcherThreads = new FetcherThread[numberOfFetcherThreads];
            for (int i = 0; i < numberOfFetcherThreads; i++) {
                fetcherThreads[i] = new FetcherThread(
                        jsoupFetcher,
                        vistedDomainCache,
                        visitedUrlsCache,
                        linkConsumer,
                        linkProducer,
                        linkPairHtmlQueue);
            }
            for (int i = 0; i < numberOfFetcherThreads; i++) {
                fetcherThreads[i].start();
            }

            ProcessorThread[] processorThreads = new ProcessorThread[numberOfProcessorThreads];
            for (int i = 0; i < numberOfProcessorThreads; i++) {
                processorThreads[i] = new ProcessorThread(
                        linkProducer,
                        elasticDao,
                        hbaseDao,
                        visitedUrlsCache,
                        linkPairHtmlQueue);
            }
            for (int i = 0; i < numberOfProcessorThreads; i++) {
                processorThreads[i].start();
            }
        } catch (HbaseSiteDaoException e) {
            logger.error(e.getMessage(), e);
        }
    }
}


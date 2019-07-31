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
import in.nimbo.fetch.HttpClientFetcher;
import in.nimbo.fetch.JsoupFetcher;
import in.nimbo.model.Pair;
import in.nimbo.kafka.LinkConsumer;
import in.nimbo.kafka.LinkProducer;
import in.nimbo.cache.RedisVisitedLinksCache;
import in.nimbo.cache.VisitedLinksCache;
import in.nimbo.cache.CaffeineVistedDomainCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.Closeable;
import java.io.File;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class App {
    private static Config config;
    private static Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        Config outConfig = ConfigFactory.parseFile(new File("config.properties"));
        Config inConfig = ConfigFactory.load("config");
        config = ConfigFactory.load(outConfig).withFallback(inConfig);
        SharedMetricRegistries.setDefault(config.getString("metric.registry.name"));
        MetricRegistry metricRegistry = SharedMetricRegistries.getDefault();
        List<Closeable> closeables = new LinkedList<>();
        ShutdownHook shutdownHook = new ShutdownHook(closeables, config);
        Runtime.getRuntime().addShutdownHook(shutdownHook);
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
                logger.error("SSl can't be established", e);
            }


            Configuration hbaseConfig = HBaseConfiguration.create();
            HbaseSiteDaoImpl hbaseDao = new HbaseSiteDaoImpl(hbaseConfig, config);
            closeables.add(hbaseDao);
            int numberOfFetcherThreads = config.getInt("fetcher.threads.num");
            HttpClientFetcher fetcher = new HttpClientFetcher(config);
            closeables.add(fetcher);

            int numberOfProcessorThreads = config.getInt("processor.threads.num");

            VisitedLinksCache visitedUrlsCache = new RedisVisitedLinksCache(config);
            closeables.add(visitedUrlsCache);
            CaffeineVistedDomainCache vistedDomainCache = new CaffeineVistedDomainCache(config);
            closeables.add(vistedDomainCache);
            ElasticSiteDaoImpl elasticDao = new ElasticSiteDaoImpl(config);
            closeables.add(elasticDao);


            LinkConsumer linkConsumer = new LinkConsumer(config);
            linkConsumer.start();
            LinkProducer linkProducer = new LinkProducer(config);
            //this shutdown hook is only for kafka
            KafkaShutdownHook kafkaShutdownHook = new KafkaShutdownHook(linkConsumer, linkProducer, config);
            Runtime.getRuntime().addShutdownHook(kafkaShutdownHook);
            LinkedBlockingQueue<Pair<String, String>> linkPairHtmlQueue = new LinkedBlockingQueue<>();
            SharedMetricRegistries.getDefault().register(
                    MetricRegistry.name(FetcherThread.class, "fetch queue size"),
                    (Gauge<Integer>) linkPairHtmlQueue::size);
            JsoupFetcher jsoupFetcher = new JsoupFetcher();

            FetcherThread[] fetcherThreads = new FetcherThread[numberOfFetcherThreads];
            for (int i = 0; i < numberOfFetcherThreads; i++) {
                fetcherThreads[i] = new FetcherThread(jsoupFetcher,
                        vistedDomainCache,
                        visitedUrlsCache,
                        linkConsumer,
                        linkProducer,
                        linkPairHtmlQueue);
                closeables.add(fetcherThreads[i]);
                fetcherThreads[i].start();
            }

            ProcessorThread[] processorThreads = new ProcessorThread[numberOfProcessorThreads];
            for (int i = 0; i < numberOfProcessorThreads; i++) {
                processorThreads[i] = new ProcessorThread(linkProducer,
                        elasticDao,
                        hbaseDao,
                        visitedUrlsCache,
                        linkPairHtmlQueue,
                        config);
                closeables.add(processorThreads[i]);
                processorThreads[i].start();
            }

        } catch (HbaseSiteDaoException e) {
            logger.error(e.getMessage(), e);
        }
    }
}


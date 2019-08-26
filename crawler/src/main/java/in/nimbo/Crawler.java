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
import in.nimbo.cache.CaffeineVistedDomainCache;
import in.nimbo.cache.HBaseVisitedLinksCache;
import in.nimbo.cache.RedisVisitedLinksCache;
import in.nimbo.dao.ElasticSiteDaoImpl;
import in.nimbo.dao.HbaseSiteDaoImpl;
import in.nimbo.fetch.JsoupFetcher;
import in.nimbo.kafka.LinkConsumer;
import in.nimbo.kafka.LinkProducer;
import in.nimbo.model.Pair;
import in.nimbo.model.Site;
import in.nimbo.shutdown_hook.HbaseCacheShutdownHook;
import in.nimbo.shutdown_hook.HbaseShutdownHook;
import in.nimbo.shutdown_hook.KafkaShutdownHook;
import in.nimbo.shutdown_hook.ShutdownHook;
import in.nimbo.util.ThreadManager;
import in.nimbo.util.ZkConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class Crawler {
    private static Logger logger = LoggerFactory.getLogger(Crawler.class);
    private Config config;
    private ZkConfig zkConfig;
    private MetricRegistry metricRegistry;
    private List<Closeable> closeables = new LinkedList<>();
    private List<String> acceptableLanguages;

    public void start() {
        initMetrics();
        Timer appInitializingMetric = metricRegistry.timer("app initializing");
        try (Timer.Context appInitializingTimer = appInitializingMetric.time()) {
            loadConfig();
            ShutdownHook shutdownHook = new ShutdownHook(closeables);
            Runtime.getRuntime().addShutdownHook(shutdownHook);
            initLanguageDetector();
            disableSsl();

            LinkedBlockingQueue<Site> hbaseBulkQueue = new LinkedBlockingQueue<>();
            SharedMetricRegistries.getDefault().register(
                    MetricRegistry.name(HbaseSiteDaoImpl.class, "bulk queue size"),
                    (Gauge<Integer>) hbaseBulkQueue::size);
            LinkedBlockingQueue<String> hbaseCacheBulkQueue = new LinkedBlockingQueue<>();
            SharedMetricRegistries.getDefault().register(
                    MetricRegistry.name(HbaseSiteDaoImpl.class, "cache bulk queue size"),
                    (Gauge<Integer>) hbaseCacheBulkQueue::size);

            createThreadManager(hbaseBulkQueue, hbaseCacheBulkQueue);

            createHbaseDaos(hbaseBulkQueue, hbaseCacheBulkQueue);
        } catch (IOException e) {
            logger.error("Can't create connection to hbase!", e);
        }
    }

    private void createThreadManager(LinkedBlockingQueue<Site> hbaseBulkQueue,
                                     LinkedBlockingQueue<String> hbaseCacheBulkQueue) {
        RedisVisitedLinksCache visitedUrlsCache = new RedisVisitedLinksCache(config);
        closeables.add(visitedUrlsCache);
        CaffeineVistedDomainCache visitedDomainCache = new CaffeineVistedDomainCache(config);
        ElasticSiteDaoImpl elasticDao = new ElasticSiteDaoImpl(config);
        closeables.add(elasticDao);

        LinkConsumer linkConsumer = new LinkConsumer(config);
        linkConsumer.start();
        LinkProducer linkProducer = new LinkProducer(config);
        KafkaShutdownHook kafkaShutdownHook = new KafkaShutdownHook(linkConsumer, linkProducer);
        Runtime.getRuntime().addShutdownHook(kafkaShutdownHook);

        LinkedBlockingQueue<Pair<String, String>> linkPairHtmlQueue = new LinkedBlockingQueue<>(
                config.getInt("queue.link.pair.html.size"));
        SharedMetricRegistries.getDefault().register(
                MetricRegistry.name(FetcherThread.class, "fetch queue size"),
                (Gauge<Integer>) linkPairHtmlQueue::size);

        JsoupFetcher jsoupFetcher = new JsoupFetcher(config);

        ThreadManager threadManager = new ThreadManager(zkConfig, linkProducer, elasticDao, visitedUrlsCache, linkPairHtmlQueue, hbaseBulkQueue,
                acceptableLanguages, jsoupFetcher, visitedDomainCache, linkConsumer, hbaseCacheBulkQueue);
        closeables.add(threadManager);
    }

    private void createHbaseDaos(LinkedBlockingQueue<Site> hbaseBulkQueue,
                                 LinkedBlockingQueue<String> hbaseCacheBulkQueue) throws IOException {
        Configuration hbaseConfig = HBaseConfiguration.create();
        final Connection hbaseConnection = ConnectionFactory.createConnection(hbaseConfig);
        int numberOfHbaseThreads = config.getInt("hbase.threads.num");
        HbaseSiteDaoImpl[] hbaseSiteDaoImpls = new HbaseSiteDaoImpl[numberOfHbaseThreads];
        HbaseShutdownHook hbaseShutdownHook = new HbaseShutdownHook(hbaseSiteDaoImpls);
        Runtime.getRuntime().addShutdownHook(hbaseShutdownHook);
        for (int i = 0; i < numberOfHbaseThreads; i++) {
            hbaseSiteDaoImpls[i] = new HbaseSiteDaoImpl(hbaseConnection, hbaseBulkQueue, config);
            hbaseSiteDaoImpls[i].start();
        }
        HBaseVisitedLinksCache[] hBaseVisitedLinksCaches = new HBaseVisitedLinksCache[numberOfHbaseThreads];
        HbaseCacheShutdownHook hbaseCacheShutdownHook = new HbaseCacheShutdownHook(hBaseVisitedLinksCaches);
        Runtime.getRuntime().addShutdownHook(hbaseCacheShutdownHook);
        for (int i = 0; i < numberOfHbaseThreads; i++) {
            hBaseVisitedLinksCaches[i] = new HBaseVisitedLinksCache(hbaseConnection, hbaseCacheBulkQueue, config);
            hBaseVisitedLinksCaches[i].start();
        }
    }

    private void initMetrics() {
        metricRegistry = SharedMetricRegistries.getDefault();
        JmxReporter jmxReporter = JmxReporter.forRegistry(metricRegistry).inDomain("crawler").build();
        jmxReporter.start();
    }

    private void initLanguageDetector() {
        try {
            DetectorFactory.loadProfile(config.getString("langDetect.profile.dir"));
        } catch (LangDetectException e) {
            logger.error("langDetector profile can't be loaded, lang detection not started", e);
        }
        String acceptableLanguagesString = config.getString("langDetect.acceptable.languages");
        acceptableLanguages = Arrays.asList(acceptableLanguagesString.split(","));
    }

    private void disableSsl() {
        try {
            TrustManager[] trustAllCerts = new TrustManager[]{new X509TrustManager() {
                public X509Certificate[] getAcceptedIssuers() {
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
    }

    private void loadConfig() {
        Config outConfig = ConfigFactory.parseFile(new File("config.properties"));
        Config inConfig = ConfigFactory.load("config");
        try {
            zkConfig = new ZkConfig(ConfigFactory.load(outConfig).withFallback(inConfig));
        } catch (IOException e) {
            logger.error("cannot connect too zookeeper", e);
            System.exit(1);
        }
        config = zkConfig.getConfig();
    }
}


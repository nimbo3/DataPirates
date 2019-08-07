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
import in.nimbo.cache.RedisVisitedLinksCache;
import in.nimbo.dao.ElasticSiteDaoImpl;
import in.nimbo.dao.HbaseSiteDaoImpl;
import in.nimbo.fetch.JsoupFetcher;
import in.nimbo.kafka.LinkConsumer;
import in.nimbo.kafka.LinkProducer;
import in.nimbo.model.Pair;
import in.nimbo.model.Site;
import in.nimbo.shutdown_hook.HbaseShutdownHook;
import in.nimbo.shutdown_hook.KafkaShutdownHook;
import in.nimbo.shutdown_hook.ShutdownHook;
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
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class App {
    private static Config config;
    private static Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        loadConfig();

        SharedMetricRegistries.setDefault("data-pirates-crawler");
        MetricRegistry metricRegistry = SharedMetricRegistries.getDefault();
        List<Closeable> closeables = new LinkedList<>();
        ShutdownHook shutdownHook = new ShutdownHook(closeables);
        Runtime.getRuntime().addShutdownHook(shutdownHook);
        JmxReporter jmxReporter = JmxReporter.forRegistry(metricRegistry).inDomain("crawler").build();
        jmxReporter.start();
        Timer appInitializingMetric = metricRegistry.timer("app initializing");
        try (Timer.Context appInitializingTimer = appInitializingMetric.time()) {
            try {
                DetectorFactory.loadProfile(config.getString("langDetect.profile.dir"));
            } catch (LangDetectException e) {
                logger.error("langDetector profile can't be loaded, lang detection not started", e);
            }
            fixSsl();

            LinkedBlockingQueue<Site> hbaseBulkQueue = new LinkedBlockingQueue<>();
            SharedMetricRegistries.getDefault().register(
                    MetricRegistry.name(HbaseSiteDaoImpl.class, "bulk queue size"),
                    (Gauge<Integer>) hbaseBulkQueue::size);

            Configuration hbaseConfig = HBaseConfiguration.create();

            final Connection conn = ConnectionFactory.createConnection(hbaseConfig);
            int numberOfFetcherThreads = config.getInt("fetcher.threads.num");
            int numberOfProcessorThreads = config.getInt("processor.threads.num");
            int numberOfHbaseThreads = config.getInt("hbase.threads.num");

            RedisVisitedLinksCache visitedUrlsCache = new RedisVisitedLinksCache(config);
            closeables.add(visitedUrlsCache);
            CaffeineVistedDomainCache vistedDomainCache = new CaffeineVistedDomainCache(config);
            ElasticSiteDaoImpl elasticDao = new ElasticSiteDaoImpl(config);
            closeables.add(elasticDao);


            LinkConsumer linkConsumer = new LinkConsumer(config);
            linkConsumer.start();
            LinkProducer linkProducer = new LinkProducer(config);
            //this shutdown hook is only for kafka
            KafkaShutdownHook kafkaShutdownHook = new KafkaShutdownHook(linkConsumer, linkProducer);
            Runtime.getRuntime().addShutdownHook(kafkaShutdownHook);
            LinkedBlockingQueue<Pair<String, String>> linkPairHtmlQueue = new LinkedBlockingQueue<>(
                    config.getInt("queue.link.pair.html.size"));
            SharedMetricRegistries.getDefault().register(
                    MetricRegistry.name(FetcherThread.class, "fetch queue size"),
                    (Gauge<Integer>) linkPairHtmlQueue::size);
            JsoupFetcher jsoupFetcher = new JsoupFetcher(config);


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

            HbaseSiteDaoImpl[] hbaseSiteDaoImpls = new HbaseSiteDaoImpl[numberOfHbaseThreads];
            HbaseShutdownHook hbaseShutdownHook = new HbaseShutdownHook(hbaseSiteDaoImpls);
            Runtime.getRuntime().addShutdownHook(hbaseShutdownHook);
            for (int i = 0; i < numberOfHbaseThreads; i++) {
                hbaseSiteDaoImpls[i] = new HbaseSiteDaoImpl(conn, hbaseBulkQueue, hbaseConfig, config);
                hbaseSiteDaoImpls[i].start();
            }

            ProcessorThread[] processorThreads = new ProcessorThread[numberOfProcessorThreads];
            for (int i = 0; i < numberOfProcessorThreads; i++) {
                processorThreads[i] = new ProcessorThread(linkProducer,
                        elasticDao,
                        visitedUrlsCache,
                        linkPairHtmlQueue,
                        hbaseBulkQueue);
                closeables.add(processorThreads[i]);
                processorThreads[i].start();
            }
        } catch (IOException e) {
            logger.error("Can't create connection to hbase!", e);
        }
    }

    private static void fixSsl() {
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

    private static void loadConfig() {
        Config outConfig = ConfigFactory.parseFile(new File("config.properties"));
        Config inConfig = ConfigFactory.load("config");
        config = ConfigFactory.load(outConfig).withFallback(inConfig);
    }
}


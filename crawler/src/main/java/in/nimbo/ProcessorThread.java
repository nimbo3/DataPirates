package in.nimbo;

import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.typesafe.config.Config;
import in.nimbo.cache.VisitedLinksCache;
import in.nimbo.dao.ElasticSiteDaoImpl;
import in.nimbo.exception.SiteDaoException;
import in.nimbo.kafka.LinkProducer;
import in.nimbo.model.Pair;
import in.nimbo.model.Site;
import in.nimbo.parser.Parser;
import in.nimbo.util.UnusableSiteDetector;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.util.concurrent.LinkedBlockingQueue;

class ProcessorThread extends Thread implements Closeable {
    private static Logger logger = Logger.getLogger(ProcessorThread.class);
    private static Timer crawlTimer = SharedMetricRegistries.getDefault().timer("processor thread");
    private final Config config;
    private LinkProducer linkProducer;
    private ElasticSiteDaoImpl elasitcSiteDao;
    private LinkedBlockingQueue<Site> hbaseBulkQueue;
    private LinkedBlockingQueue<Pair<String, String>> linkPairHtmlQueue;
    private VisitedLinksCache visitedUrlsCache;
    private boolean closed = false;

    public ProcessorThread(LinkProducer linkProducer, ElasticSiteDaoImpl elasticSiteDao,
                           VisitedLinksCache visitedUrlsCache,
                           LinkedBlockingQueue<Pair<String, String>> linkPairHtmlQueue,
                           LinkedBlockingQueue<Site> hbaseBulkQueue,
                           Config config) {
        this.linkProducer = linkProducer;
        this.elasitcSiteDao = elasticSiteDao;
        this.linkPairHtmlQueue = linkPairHtmlQueue;
        this.visitedUrlsCache = visitedUrlsCache;
        this.config = config;
        this.hbaseBulkQueue = hbaseBulkQueue;
    }

    @Override
    public void run() {
        try {
            while (!interrupted() && !closed) {
                try (Timer.Context time = crawlTimer.time()) {
                    Pair<String, String> pair = null;
                    try {
                        pair = linkPairHtmlQueue.take();
                    } catch (InterruptedException e) {
                        logger.error("InterruptedException happened while polling from pair", e);
                        Thread.currentThread().interrupt();
                    }
                    String url = pair.getKey();
                    String html = pair.getValue();
                    Site site = null;

                    logger.trace(String.format("Parsing (%s)", url));
                    try {
                        Parser parser = new Parser(url, html, config);
                        site = parser.parse();
                        logger.trace(String.format("(%s) Parsed", url));
                        if (UnusableSiteDetector.hasAcceptableLanguage(site.getPlainText())) {
                            logger.trace(String.format("Putting %d anchors in Kafka(%s)", site.getAnchors().size(), url));
                            site.getAnchors().keySet().forEach(link -> {
                                if (!visitedUrlsCache.hasVisited(link)) {
                                    linkProducer.send(link);
                                }
                            });
                            logger.trace(String.format("anchors in Kafka putted(%s)", url));
                            logger.trace(String.format("(%s) Inserting into elastic", url));
                            elasitcSiteDao.insert(site);
                            logger.trace(String.format("(%s) Inserting into hbase", url));
                            hbaseBulkQueue.put(site);
                            logger.trace("Inserted : " + site.getTitle() + " : " + site.getLink());
                        }
                    } catch (SiteDaoException e) {
                        logger.error(String.format("Failed to save in database(s) : %s", url), e);
                    } catch (InterruptedException e) {
                        logger.error("hbase bulk can't take site from blocking queue!");
                    } catch (Exception e) {
                        logger.error(String.format("Failed to parse : %s", url), e);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Processor Thread Shut Down", e);
        }
    }

    @Override
    public void close() {
        closed = true;
    }
}

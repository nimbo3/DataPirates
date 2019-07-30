package in.nimbo;

import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import in.nimbo.dao.ElasticSiteDaoImpl;
import in.nimbo.dao.HbaseSiteDaoImpl;
import in.nimbo.exception.SiteDaoException;
import in.nimbo.model.Pair;
import in.nimbo.model.Site;
import in.nimbo.parser.Parser;
import in.nimbo.util.LinkProducer;
import in.nimbo.util.UnusableSiteDetector;
import in.nimbo.util.VisitedLinksCache;
import org.apache.log4j.Logger;

import java.util.concurrent.LinkedBlockingQueue;

class ProcessorThread extends Thread {
    private static Logger logger = Logger.getLogger(ProcessorThread.class);
    private static Timer crawlTimer = SharedMetricRegistries.getDefault().timer("processor thread");


    private LinkProducer linkProducer;
    private ElasticSiteDaoImpl elasitcSiteDao;
    private HbaseSiteDaoImpl hbaseSiteDao;
    private LinkedBlockingQueue<Pair<String, String>> linkPairHtmlQueue;
    private VisitedLinksCache visitedUrlsCache;

    public ProcessorThread(LinkProducer linkProducer, ElasticSiteDaoImpl elasticSiteDao,
                           HbaseSiteDaoImpl hbaseSiteDao, VisitedLinksCache visitedUrlsCache,
                           LinkedBlockingQueue<Pair<String, String>> linkPairHtmlQueue) {
        this.linkProducer = linkProducer;
        this.elasitcSiteDao = elasticSiteDao;
        this.hbaseSiteDao = hbaseSiteDao;
        this.linkPairHtmlQueue = linkPairHtmlQueue;
        this.visitedUrlsCache = visitedUrlsCache;
    }

    @Override
    public void run() {
        try {
            while (!interrupted()) {
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
                        Parser parser = new Parser(url, html);
                        site = parser.parse();
                        logger.trace(String.format("(%s) Parsed", url));
                        if (new UnusableSiteDetector(site.getPlainText()).hasAcceptableLanguage()) {
                            logger.trace(String.format("Putting %d anchors in Kafka(%s)", site.getAnchors().size(), url));
                            site.getAnchors().keySet().forEach(link -> {
                                if (!visitedUrlsCache.hasVisited(link))
                                    linkProducer.send(link);
                            });
                            logger.trace(String.format("anchors in Kafka putted(%s)", url));
                            logger.trace(String.format("(%s) Inserting into elastic", url));
                            elasitcSiteDao.insert(site);
                            logger.trace(String.format("(%s) Inserting into hbase", url));
                            hbaseSiteDao.insert(site);
                            logger.trace("Inserted : " + site.getTitle() + " : " + site.getLink());
                        }
                    } catch (SiteDaoException e) {
                        logger.error(String.format("Failed to save in database(s) : %s", url), e);
                        hbaseSiteDao.delete(site.getReverseLink());
                        elasitcSiteDao.delete(url);
                    } catch (Exception e) {
                        logger.error(String.format("Failed to parse : %s", url), e);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Processor Thread Shut Down", e);
        }
    }
}

package in.nimbo;

import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import in.nimbo.dao.ElasticSiteDaoImpl;
import in.nimbo.dao.HbaseSiteDaoImpl;
import in.nimbo.exception.FetchException;
import in.nimbo.exception.SiteDaoException;
import in.nimbo.model.Pair;
import in.nimbo.model.Site;
import in.nimbo.parser.Parser;
import in.nimbo.util.LinkProducer;
import in.nimbo.util.UnusableSiteDetector;
import in.nimbo.util.VisitedLinksCache;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.concurrent.LinkedBlockingQueue;

class ProcessorThread extends Thread {
    private static Logger logger = Logger.getLogger(ProcessorThread.class);
    private static Timer crawlTimer = SharedMetricRegistries.getDefault().timer("crawler thread");

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
                try {
                    logger.debug(String.format("Parsing (%s)", url));
                    Parser parser = new Parser(url, html);
                    site = parser.parse();
                    logger.debug(String.format("(%s) Parsed", url));
                    if (new UnusableSiteDetector(site.getPlainText()).hasAcceptableLanguage()) {
                        logger.debug(String.format("Putting %d anchors in Kafka(%s)", site.getAnchors().size(), url));
                        site.getAnchors().keySet().parallelStream().forEach(link -> {
                            if (!visitedUrlsCache.hasVisited(link))
                                linkProducer.send(link);
                        });
                        logger.debug(String.format("anchors in Kafka putted(%s)", url));
                        logger.debug(String.format("(%s) Inserting into elastic", url));
                        elasitcSiteDao.insert(site);
                        logger.debug(String.format("(%s) Inserting into hbase", url));
                        hbaseSiteDao.insert(site);
                        logger.debug("Inserted : " + site.getTitle() + " : " + site.getLink());
                    }
                } catch (SiteDaoException e) {
                    logger.error(String.format("Failed to save in database(s) : %s", url), e);
                    hbaseSiteDao.delete(site.getReverseLink());
                    elasitcSiteDao.delete(url);
                }
            }
        }
    }
}

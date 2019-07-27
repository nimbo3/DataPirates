package in.nimbo;

import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import in.nimbo.database.dao.ElasticSiteDaoImpl;
import in.nimbo.database.dao.HbaseSiteDaoImpl;
import in.nimbo.exception.SiteDaoException;
import in.nimbo.model.Site;
import in.nimbo.parser.Parser;
import in.nimbo.util.LinkConsumer;
import in.nimbo.util.LinkProducer;
import in.nimbo.util.UnusableSiteDetector;
import in.nimbo.util.VisitedLinksCache;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.io.IOException;

class CrawlerThread extends Thread {
    private static Logger logger = Logger.getLogger(CrawlerThread.class);
    private static Timer crawlTimer = SharedMetricRegistries.getDefault().timer("crawler");
    private FetcherImpl fetcher;
    private VisitedLinksCache visitedDomainsCache;
    private VisitedLinksCache visitedUrlsCache;
    private LinkConsumer linkConsumer;
    private LinkProducer linkProducer;
    private ElasticSiteDaoImpl elasitcSiteDao;
    private HbaseSiteDaoImpl hbaseSiteDao;

    public CrawlerThread(FetcherImpl fetcher,
                         VisitedLinksCache visitedDomainsCache, VisitedLinksCache visitedUrlsCache,
                         LinkConsumer linkConsumer, LinkProducer linkProducer, ElasticSiteDaoImpl elasticSiteDao, HbaseSiteDaoImpl hbaseSiteDao) {
        this.fetcher = fetcher;
        this.visitedDomainsCache = visitedDomainsCache;
        this.linkConsumer = linkConsumer;
        this.linkProducer = linkProducer;
        this.visitedUrlsCache = visitedUrlsCache;
        this.elasitcSiteDao = elasticSiteDao;
        this.hbaseSiteDao = hbaseSiteDao;
    }

    @Override
    public void run() {
        while (!interrupted()) {
            try (Timer.Context time = crawlTimer.time()) {
                String url = null;
                try {
                    url = linkConsumer.pop();
                } catch (InterruptedException e) {
                    logger.error("InterruptedException happened while consuming from Kafka", e);
                }
                logger.info(String.format("New link (%s) poped from queue", url));
                if (!visitedDomainsCache.hasVisited(Parser.getDomain(url)) &&
                        !visitedUrlsCache.hasVisited(url)) {
                    try {
                        fetcher.fetch(url);
                        if (fetcher.isContentTypeTextHtml()) {
                            Parser parser = new Parser(url, fetcher.getRawHtmlDocument());
                            Site site = parser.parse();
                            if (new UnusableSiteDetector(site.getPlainText()).hasAcceptableLanguage()) {
                                visitedDomainsCache.put(Parser.getDomain(url));
                                //Todo : Check In redis And Then Put
                                site.getAnchors().keySet().forEach(link -> linkProducer.send(link));
                                visitedUrlsCache.put(url);
                                elasitcSiteDao.insert(site);
                                hbaseSiteDao.insert(site);
                                logger.info(site.getTitle() + " : " + site.getLink());
                            }
                        }
                    } catch (IOException e) {
                        logger.error(String.format("url: %s", url), e);
                    } catch (SiteDaoException e) {
                        logger.error(String.format("Failed to save in database(s) : %s", url), e);
                    }
                } else {
                    linkProducer.send(url);
                    logger.info(String.format("New link (%s) pushed to queue", url));
                }
            }
        }
    }
}

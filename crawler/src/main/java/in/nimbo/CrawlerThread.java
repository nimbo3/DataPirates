package in.nimbo;

import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import in.nimbo.dao.ElasticSiteDaoImpl;
import in.nimbo.dao.HbaseSiteDaoImpl;
import in.nimbo.exception.FetchException;
import in.nimbo.exception.SiteDaoException;
import in.nimbo.model.Site;
import in.nimbo.parser.Parser;
import in.nimbo.util.LinkConsumer;
import in.nimbo.util.LinkProducer;
import in.nimbo.util.UnusableSiteDetector;
import in.nimbo.util.VisitedLinksCache;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.net.MalformedURLException;

class CrawlerThread extends Thread implements Closeable {
    private static Logger logger = Logger.getLogger(CrawlerThread.class);
    private static Timer crawlTimer = SharedMetricRegistries.getDefault().timer("crawler");
    private FetcherImpl fetcher;
    private VisitedLinksCache visitedDomainsCache;
    private VisitedLinksCache visitedUrlsCache;
    private LinkConsumer linkConsumer;
    private LinkProducer linkProducer;
    private ElasticSiteDaoImpl elasitcSiteDao;
    private HbaseSiteDaoImpl hbaseSiteDao;
    private boolean closed = false;

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

    /**
     * Note:
     * before starting the crawler there are some links in Queue
     * these links are not necessarily normalized neither added to redis
     * and in the code below these links are not taken care of
     *
     * if you want this code to work correctly,
     * these links should be normalized and then put into redis.
     * then you're good to go :))
     *
     * assumption:
     * all the links in kafka are unique and normalized
     * (as a result we won't check links before fetching them and we will check them before adding them to kafka
     * and consequently we won't add fetching url to redis because it already exists there)
     */

    @Override
    public void run() {
        while (!closed) {
            String url = null;
            try (Timer.Context time = crawlTimer.time()) {
                try {
                    url = linkConsumer.pop();
                } catch (InterruptedException e) {
                    logger.error("InterruptedException happened while consuming from Kafka", e);
                    Thread.currentThread().interrupt();
                }
                logger.debug(String.format("New link (%s) poped from queue", url));
                if (!visitedDomainsCache.hasVisited(Parser.getDomain(url))) {
                    Site site = null;
                    try {
                        logger.debug(String.format("Fetching (%s)", url));
                        String html = fetcher.fetch(url);
                        String redirectNormalizedUrl = fetcher.getRedirectUrl();
                        logger.debug(String.format("(%s) Fetched", url));
                        if (fetcher.isContentTypeTextHtml()) {
                            logger.debug(String.format("Parsing (%s)", url));
                            Parser parser = new Parser(url, html);
                            site = parser.parse();
                            logger.debug(String.format("(%s) Parsed", url));
                            if (UnusableSiteDetector.hasAcceptableLanguage(site.getPlainText())) {
                                visitedDomainsCache.put(Parser.getDomain(url));

                                if (!url.equals(redirectNormalizedUrl))
                                    visitedUrlsCache.put(redirectNormalizedUrl);

                                logger.debug(String.format("Putting %d anchors in Kafka(%s)", site.getAnchors().size(), url));
                                site.getAnchors().keySet().parallelStream().forEach(link -> {
                                    if (!visitedUrlsCache.hasVisited(link)) {
                                        visitedUrlsCache.put(link);
                                        linkProducer.send(link);
                                    }
                                });
                                logger.debug(String.format("anchors in Kafka putted(%s)", url));
                                logger.debug(String.format("(%s) Inserting into elastic", url));
                                elasitcSiteDao.insert(site);
                                logger.debug(String.format("(%s) Inserting into hbase", url));
                                hbaseSiteDao.insert(site);
                                logger.debug("Inserted : " + site.getTitle() + " : " + site.getLink());
                            }
                        }
                    } catch (IOException | FetchException e) {
                        logger.error(e.getMessage(), e);
                    } catch (SiteDaoException e) {
                        logger.error(String.format("Failed to save in database(s) : %s", url), e);
                        hbaseSiteDao.delete(site.getReverseLink());
                        elasitcSiteDao.delete(url);
                    }
                } else {
                    linkProducer.send(url);
                    logger.debug(String.format("New link (%s) pushed to queue", url));
                }
            } catch (MalformedURLException e) {
                logger.error("can't get domain for link: " + url, e);
            }
        }
    }

    @Override
    public void close() {
        closed = true;
    }
}

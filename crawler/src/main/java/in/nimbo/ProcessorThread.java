package in.nimbo;

import com.codahale.metrics.Meter;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import in.nimbo.cache.VisitedLinksCache;
import in.nimbo.dao.ElasticSiteDaoImpl;
import in.nimbo.exception.SiteDaoException;
import in.nimbo.kafka.LinkProducer;
import in.nimbo.model.Pair;
import in.nimbo.model.Site;
import in.nimbo.parser.Parser;
import in.nimbo.util.HashCodeGenerator;
import in.nimbo.util.LanguageDetector;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

class ProcessorThread extends Thread implements Closeable {
    private static Logger logger = Logger.getLogger(ProcessorThread.class);
    private static Timer crawlTimer = SharedMetricRegistries.getDefault().timer("processor thread");
    private static Timer kafkaAnchorsPutTimer = SharedMetricRegistries.getDefault().timer("kafka anchors put");
    private static Meter langDetectorSkips = SharedMetricRegistries.getDefault().meter("language detector skips");
    private static Meter redisAnchorSkips = SharedMetricRegistries.getDefault().meter("redis anchors skips");
    private static Meter redisAnchorMatches = SharedMetricRegistries.getDefault().meter("redis anchors fetched-before matches");
    private LinkProducer linkProducer;
    private ElasticSiteDaoImpl elasitcSiteDao;
    private LinkedBlockingQueue<Site> hbaseBulkQueue;
    private LinkedBlockingQueue<Pair<String, String>> linkPairHtmlQueue;
    private VisitedLinksCache visitedUrlsCache;
    private boolean closed = false;
    private List<String> acceptableLanguages;

    public ProcessorThread(LinkProducer linkProducer, ElasticSiteDaoImpl elasticSiteDao,
                           VisitedLinksCache visitedUrlsCache,
                           LinkedBlockingQueue<Pair<String, String>> linkPairHtmlQueue,
                           LinkedBlockingQueue<Site> hbaseBulkQueue,
                           List<String> acceptableLanguages) {
        this.linkProducer = linkProducer;
        this.elasitcSiteDao = elasticSiteDao;
        this.linkPairHtmlQueue = linkPairHtmlQueue;
        this.visitedUrlsCache = visitedUrlsCache;
        this.hbaseBulkQueue = hbaseBulkQueue;
        this.acceptableLanguages = acceptableLanguages;
    }

    @Override
    public void run() {
        try {
            while (!interrupted() && !closed) {
                try (Timer.Context time = crawlTimer.time()) {
                    Pair<String, String> pair = null;
                    pair = linkPairHtmlQueue.take();
                    String url = pair.getKey();
                    String html = pair.getValue();
                    try {
                        Parser parser = new Parser(url, html);
                        Site site = parser.parse();
                        String language = LanguageDetector.detect(site.getPlainText());
                        if (acceptableLanguages.contains(language)) {
                            site.setLanguage(language);
                            elasitcSiteDao.insert(site);
                            if (site.getLanguage().equals("en"))
                                hbaseBulkQueue.put(site);
                            logger.trace("Inserted In DBs: " + site.getTitle() + " : " + site.getLink());
                            logger.trace(String.format("Putting %d anchors in Kafka(%s)", site.getAnchors().size(), url));
                            try (Timer.Context kafkaPutTime = kafkaAnchorsPutTimer.time()){
                                site.getAnchors().keySet().forEach(link -> {
                                    if (!visitedUrlsCache.hasVisited(HashCodeGenerator.md5HashString(link))) {
                                        linkProducer.send(link);
                                        redisAnchorSkips.mark();
                                    } else {
                                        redisAnchorMatches.mark();
                                    }
                                });
                            }
                            logger.trace(String.format("anchors in Kafka putted(%s)", url));
                        } else {
                            langDetectorSkips.mark();
                        }
                    } catch (SiteDaoException e) {
                        logger.error(String.format("Failed to save in database(s) : %s", url), e);
                    } catch (InterruptedException e) {
                        logger.error("hbase bulk can't take site from blocking queue!", e);
                        Thread.currentThread().interrupt();
                    } catch (MalformedURLException e) {
                        logger.error("parser can't parse url: " + url, e);
                    }catch (ProtocolException e){
                        logger.error(e.getMessage(), e);
                    }catch (Exception e) {
                        logger.error("exception thrown while processing", e);
                    }
                } catch (InterruptedException e) {
                    logger.error("can't take from linkPairHtmlQueue!", e);
                    Thread.currentThread().interrupt();
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

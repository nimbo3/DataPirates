package in.nimbo;

import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import in.nimbo.exception.FetchException;
import in.nimbo.model.Pair;
import in.nimbo.model.Site;
import in.nimbo.parser.Parser;
import in.nimbo.util.LinkConsumer;
import in.nimbo.util.LinkProducer;
import in.nimbo.util.VisitedLinksCache;
import org.apache.log4j.Logger;

import java.net.MalformedURLException;
import java.util.concurrent.LinkedBlockingQueue;

public class FetcherThread extends Thread {
    private static Logger logger = Logger.getLogger(FetcherThread.class);
    private static Timer fetcherTimer = SharedMetricRegistries.getDefault().timer("fetcher thread");

    private FetcherImpl fetcher;
    private VisitedLinksCache visitedDomainsCache;
    private VisitedLinksCache visitedUrlsCache;
    private LinkConsumer linkConsumer;
    private LinkProducer linkProducer;
    private LinkedBlockingQueue<Pair<String, String>> linkPairHtmlQueue;

    public FetcherThread(FetcherImpl fetcher,
                         VisitedLinksCache visitedDomainsCache, VisitedLinksCache visitedUrlsCache,
                         LinkConsumer linkConsumer, LinkProducer linkProducer,
                         LinkedBlockingQueue<Pair<String, String>> linkPairHtmlQueue) {
        this.fetcher = fetcher;
        this.visitedDomainsCache = visitedDomainsCache;
        this.linkConsumer = linkConsumer;
        this.linkProducer = linkProducer;
        this.visitedUrlsCache = visitedUrlsCache;
        this.linkPairHtmlQueue = linkPairHtmlQueue;
    }

    @Override
    public void run() {
        try {
            while (!interrupted()) {
                String url = null;
                try (Timer.Context time = fetcherTimer.time()) {
                    try {
                        url = linkConsumer.pop();
                    } catch (InterruptedException e) {
                        logger.error("InterruptedException happened while consuming from Kafka", e);
                        Thread.currentThread().interrupt();
                    }
                    if (visitedUrlsCache.hasVisited(url))
                        continue;
                    logger.trace(String.format("New link (%s) poped from queue", url));
                    if (!visitedDomainsCache.hasVisited(Parser.getDomain(url))) {
                        Site site = null;
                        logger.trace(String.format("Fetching (%s)", url));
                        try {
                            String html = fetcher.fetch(url);
                            logger.trace(String.format("(%s) Fetched", url));
                            if (fetcher.isContentTypeTextHtml()) {
                                Pair<String, String> pair = new Pair<>(url, html);
                                try {
                                    linkPairHtmlQueue.put(pair);
                                    visitedUrlsCache.put(url);
                                    visitedDomainsCache.put(Parser.getDomain(url));
                                } catch (InterruptedException e) {
                                    logger.error("Interrupted Exception when putting in linkPairHtmlQueue", e);
                                }
                            }
                        } catch (FetchException e) {
                            logger.error(e.getMessage(), e);
                        } catch (Exception e) {
                            logger.error("Exception In Fetching ", e);
                        }
                    } else {
                        linkProducer.send(url);
                        logger.trace(String.format("New link (%s) pushed to queue", url));
                    }
                } catch (MalformedURLException e) {
                    logger.error("can't get domain for link: " + url, e);
                }
            }
        } catch (Exception e) {
            logger.error("Fetcher Thread Shut Down", e);
        }
    }
}

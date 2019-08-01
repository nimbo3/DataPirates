package in.nimbo;

import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import in.nimbo.cache.VisitedLinksCache;
import in.nimbo.exception.FetchException;
import in.nimbo.fetch.Fetcher;
import in.nimbo.kafka.LinkConsumer;
import in.nimbo.kafka.LinkProducer;
import in.nimbo.model.Pair;
import in.nimbo.model.Site;
import in.nimbo.parser.Parser;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.net.MalformedURLException;
import java.util.concurrent.LinkedBlockingQueue;

public class FetcherThread extends Thread implements Closeable {
    private static Logger logger = Logger.getLogger(FetcherThread.class);
    private static Timer fetcherTimer = SharedMetricRegistries.getDefault().timer("fetcher thread");

    private Fetcher fetcher;
    private VisitedLinksCache visitedDomainsCache;
    private VisitedLinksCache visitedUrlsCache;
    private LinkConsumer linkConsumer;
    private LinkProducer linkProducer;
    private LinkedBlockingQueue<Pair<String, String>> linkPairHtmlQueue;
    private boolean closed = false;

    public FetcherThread(Fetcher fetcher,
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
            while (!interrupted() && !closed) {
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
                        logger.trace(String.format("Fetching (%s)", url));
                        try {
                            String html = fetcher.fetch(url);
                            logger.trace(String.format("(%s) Fetched", url));
                            if (fetcher.isContentTypeTextHtml()) {
                                Pair<String, String> pair = new Pair<>(fetcher.getRedirectUrl(), html);
                                try {
                                    linkPairHtmlQueue.put(pair);
                                    visitedUrlsCache.put(url);
                                    visitedDomainsCache.put(Parser.getDomain(url));
                                    visitedDomainsCache.put(Parser.getDomain(fetcher.getRedirectUrl()));
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

    @Override
    public void close() {
        closed = true;
    }
}

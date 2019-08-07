package in.nimbo;

import com.codahale.metrics.Meter;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import in.nimbo.cache.VisitedLinksCache;
import in.nimbo.exception.FetchException;
import in.nimbo.fetch.Fetcher;
import in.nimbo.kafka.LinkConsumer;
import in.nimbo.kafka.LinkProducer;
import in.nimbo.model.Pair;
import in.nimbo.parser.Parser;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.net.MalformedURLException;
import java.util.concurrent.LinkedBlockingQueue;

public class FetcherThread extends Thread implements Closeable {
    private static Logger logger = Logger.getLogger(FetcherThread.class);
    private Timer fetcherTimer = SharedMetricRegistries.getDefault().timer("fetcher thread");
    private Meter visitedLinksSkips = SharedMetricRegistries.getDefault().meter("fetcher visited links skips");
    private Meter visitedDomainsSkips = SharedMetricRegistries.getDefault().meter("fetcher visited domains skips");
    private Meter linkPairHtmlPutsMeter = SharedMetricRegistries.getDefault().meter("fetcher linkpairhtml puts");

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
                    if (visitedUrlsCache.hasVisited(url)) {
                        visitedLinksSkips.mark();
                        continue;
                    }
                    logger.trace(String.format("New link [%s] poped from kafka queue", url));
                    if (!visitedDomainsCache.hasVisited(Parser.getDomain(url))) {
                        try {
                            String html = fetcher.fetch(url);
                            Pair<String, String> pair = new Pair<>(fetcher.getRedirectedUrl(), html);
                            linkPairHtmlQueue.put(pair);
                            linkPairHtmlPutsMeter.mark();
                            visitedUrlsCache.put(url);
                            visitedUrlsCache.put(fetcher.getRedirectedUrl());
                            visitedDomainsCache.put(Parser.getDomain(url));
                            visitedDomainsCache.put(Parser.getDomain(fetcher.getRedirectedUrl()));
                        } catch (FetchException e) {
                            logger.error(e.getMessage(), e);
                        } catch (InterruptedException e) {
                            logger.error("Interrupted Exception when putting in linkPairHtmlQueue", e);
                            Thread.currentThread().interrupt();
                        } catch (Exception e) {
                            logger.error("Exception In Fetching ", e);
                        }
                    } else {
                        visitedDomainsSkips.mark();
                        linkProducer.send(url);
                        logger.trace(String.format("New link (%s) pushed to queue", url));
                    }
                } catch (MalformedURLException e) {
                    logger.error("Can't get domain for link: " + url, e);
                }
            }
        } catch (Exception e) {
            logger.error("Fetcher thread shut down", e);
        }
    }

    @Override
    public void close() {
        closed = true;
    }
}

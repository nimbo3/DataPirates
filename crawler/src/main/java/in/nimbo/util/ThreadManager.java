package in.nimbo.util;

import com.typesafe.config.Config;
import in.nimbo.FetcherThread;
import in.nimbo.ProcessorThread;
import in.nimbo.cache.VisitedLinksCache;
import in.nimbo.dao.ElasticSiteDaoImpl;
import in.nimbo.fetch.Fetcher;
import in.nimbo.kafka.LinkConsumer;
import in.nimbo.kafka.LinkProducer;
import in.nimbo.model.Pair;
import in.nimbo.model.Site;

import java.io.Closeable;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class ThreadManager implements Observer<Config>, Closeable {
    private final ZkConfig zkConfig;
    private Config config;
    private LinkedList<FetcherThread> fetcherThreads = new LinkedList<>();
    private LinkedList<ProcessorThread> processorThreads = new LinkedList<>();

    private LinkProducer linkProducer;
    private ElasticSiteDaoImpl elasticDao;
    private VisitedLinksCache visitedUrlsCache;
    private LinkedBlockingQueue<Pair<String, String>> linkPairHtmlQueue;
    private LinkedBlockingQueue<Site> hbaseBulkQueue;
    private List<String> acceptableLanguages;
    private Fetcher jsoupFetcher;
    private VisitedLinksCache visitedDomainCache;
    private LinkConsumer linkConsumer;

    public ThreadManager(ZkConfig zkConfig, LinkProducer linkProducer, ElasticSiteDaoImpl elasticDao,
                         VisitedLinksCache visitedUrlsCache,
                         LinkedBlockingQueue<Pair<String, String>> linkPairHtmlQueue,
                         LinkedBlockingQueue<Site> hbaseBulkQueue, List<String> acceptableLanguages,
                         Fetcher jsoupFetcher, VisitedLinksCache visitedDomainCache, LinkConsumer linkConsumer) {
        this.linkProducer = linkProducer;
        this.elasticDao = elasticDao;
        this.visitedUrlsCache = visitedUrlsCache;
        this.linkPairHtmlQueue = linkPairHtmlQueue;
        this.hbaseBulkQueue = hbaseBulkQueue;
        this.acceptableLanguages = acceptableLanguages;
        this.jsoupFetcher = jsoupFetcher;
        this.visitedDomainCache = visitedDomainCache;
        this.linkConsumer = linkConsumer;

        this.zkConfig = zkConfig;
        this.zkConfig.addObserver(this);
        config = zkConfig.getConfig();
        updateThreadNumbers();
    }


    private void updateThreadNumbers() {
        while (fetcherThreads.size() < config.getInt("fetcher.threads.num"))
            addFetcherThread();
        while (fetcherThreads.size() > config.getInt("fetcher.threads.num"))
            fetcherThreads.pop().close();
        while (processorThreads.size() < config.getInt("processor.threads.num"))
            addProcessorThread();
        while (processorThreads.size() > config.getInt("processor.threads.num"))
            processorThreads.pop().close();
    }

    private void addFetcherThread() {
        fetcherThreads.add(new FetcherThread(jsoupFetcher,
                visitedDomainCache,
                visitedUrlsCache,
                linkConsumer,
                linkProducer,
                linkPairHtmlQueue));
        fetcherThreads.peekLast().start();
    }

    private void addProcessorThread() {
        processorThreads.add(new ProcessorThread(linkProducer,
                elasticDao,
                visitedUrlsCache,
                linkPairHtmlQueue,
                hbaseBulkQueue,
                acceptableLanguages));
        processorThreads.peekLast().start();
    }

    @Override
    public void onStateChanged(Config newState) {
        config = newState;
        updateThreadNumbers();
    }

    @Override
    public void close() {
        fetcherThreads.forEach(FetcherThread::close);
        processorThreads.forEach(ProcessorThread::close);
        zkConfig.removeObserver(this);
    }
}

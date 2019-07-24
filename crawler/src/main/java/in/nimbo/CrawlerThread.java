package in.nimbo;

import in.nimbo.database.dao.SiteDao;
import in.nimbo.model.Site;
import in.nimbo.parser.Parser;
import in.nimbo.util.LinkConsumer;
import in.nimbo.util.UnusableSiteDetector;
import in.nimbo.util.VisitedLinksCache;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.io.IOException;

class CrawlerThread extends Thread {
    private static Logger logger = Logger.getLogger(CrawlerThread.class);
    private FetcherImpl fetcher;
    private VisitedLinksCache visitedDomainsCache;
    private VisitedLinksCache visitedUrlsCache;
    private LinkConsumer linkConsumer;
    private KafkaProducer kafkaProducer;
    private SiteDao database;
    private UnusableSiteDetector unusableSiteDetector;

    public CrawlerThread(FetcherImpl fetcher,
                         VisitedLinksCache visitedDomainsCache, VisitedLinksCache visitedUrlsCache,
                         LinkConsumer linkConsumer, KafkaProducer kafkaProducer, SiteDao database) {
        this.fetcher = fetcher;
        this.visitedDomainsCache = visitedDomainsCache;
        this.database = database;
        this.linkConsumer = linkConsumer;
        this.kafkaProducer = kafkaProducer;
        this.visitedUrlsCache = visitedUrlsCache;
    }

    @Override
    public void run() {
        while (!interrupted()) {
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
                            //Todo : Check In Travis And Then Put
                            site.getAnchors().keySet().forEach(link -> kafkaProducer.send(new ProducerRecord("links", link)));
                            visitedUrlsCache.put(url);
                            database.insert(site);
                            System.out.println(site.getTitle() + " : " + site.getLink());
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    logger.error(e);
                }
            } else {
                kafkaProducer.send(new ProducerRecord("links", url));
                logger.info(String.format("New link (%s) pushed to queue", url));
            }
        }
    }
}

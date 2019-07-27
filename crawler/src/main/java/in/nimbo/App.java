package in.nimbo;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import in.nimbo.database.ElasticDaoImpl;
import in.nimbo.database.dao.SiteDao;
import in.nimbo.model.Site;
import in.nimbo.parser.Parser;
import in.nimbo.queue.LinkQueue;
import in.nimbo.util.VisitedLinksCache;
import in.nimbo.util.cacheManager.CaffeineVistedDomainCache;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class App {

    public static String getDomain(String url) {
        Pattern regex = Pattern.compile("^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\\?([^#]*))?(#(.*))?");
        Matcher matcher = regex.matcher(url);
        if (matcher.find()) {
            return matcher.group(4);
        }
        return url;
    }

    public static void main(String[] args) {
        Config config = ConfigFactory.load("config");
        FetcherImpl fetcher = new FetcherImpl();
        int threads = 100;
        CrawlerThread[] crawlerThreads = new CrawlerThread[threads];
        LinkQueue linkQueue = new LinkQueue() {
            LinkedBlockingQueue<String> links = new LinkedBlockingQueue<>();

            @Override
            public void put(String link) {
                try {
                    links.put(link);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            @Override
            public String pop() {
                try {
                    return links.take();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    return null;
                }
            }

            @Override
            public void close() throws IOException {

            }
        };
        VisitedLinksCache visitedUrlsCache = new VisitedLinksCache() {
            Map<String, Integer> visitedUrls = new ConcurrentHashMap<>();

            @Override
            public void put(String normalizedUrl) {
                visitedUrls.put(normalizedUrl, 0);
            }

            @Override
            public boolean hasVisited(String normalizedUrl) {
                return visitedUrls.keySet().contains(normalizedUrl);
            }
        };
        linkQueue.put("http://ehsan-edu.org/ehs/");
        linkQueue.put("https://cafebazaar.ir/");
        linkQueue.put("https://www.mehrnews.com/");
        linkQueue.put("http://www.sharif.ir/home");
        CaffeineVistedDomainCache vistedDomainCache = new CaffeineVistedDomainCache(config);
        ElasticDaoImpl elasticDao = new ElasticDaoImpl("slave1", 9200);
        for (int i = 0; i < threads; i++) {
            crawlerThreads[i] = new CrawlerThread(fetcher,
                    vistedDomainCache,
                    visitedUrlsCache,
                    linkQueue,
                    elasticDao);
        }
        for (int i = 0; i < threads; i++) {
            crawlerThreads[i].start();
        }
    }
}

class CrawlerThread extends Thread {
    private static Logger logger = Logger.getLogger(CrawlerThread.class);
    private FetcherImpl fetcher;
    private VisitedLinksCache visitedDomainsCache;
    private VisitedLinksCache visitedUrlsCache;
    private LinkQueue linkQueue;
    private SiteDao database;

    public CrawlerThread(FetcherImpl fetcher, VisitedLinksCache visitedDomainsCache, VisitedLinksCache visitedUrlsCache, LinkQueue linkQueue, SiteDao database) {
        this.fetcher = fetcher;
        this.visitedDomainsCache = visitedDomainsCache;
        this.database = database;
        this.linkQueue = linkQueue;
        this.visitedUrlsCache = visitedUrlsCache;
    }

    @Override
    public void run() {
        while (!interrupted()) {
            String url = linkQueue.pop();
            logger.info(String.format("New link (%s) poped from queue", url));
            if (!visitedDomainsCache.hasVisited(App.getDomain(url)) &&
                    !visitedUrlsCache.hasVisited(url)) {
                try {
                    fetcher.fetch(url);
                    if (fetcher.isContentTypeTextHtml()) {
                        Parser parser = new Parser(url, fetcher.getRawHtmlDocument());
                        Site site = parser.parse();
                        visitedDomainsCache.put(App.getDomain(url));
                        site.getAnchors().keySet().forEach(link -> linkQueue.put(link));
                        visitedUrlsCache.put(url);
                        database.insert(site);
                        System.out.println(site.getTitle() + " : " + site.getLink());
                    }
                } catch (IOException e) {
                    logger.error(e);
                }
            } else {
                linkQueue.put(url);
            }
        }
    }
}

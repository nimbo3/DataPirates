package in.nimbo.util.cacheManager;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.typesafe.config.Config;
import in.nimbo.util.VisitedLinksCache;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class CaffeineVistedDomainCache implements VisitedLinksCache {

    private Cache<String, Date> visitedSites;

    public CaffeineVistedDomainCache(Config config) {
        int politenessWaitingTime = config.getInt("politeness.waiting.time");
        visitedSites = Caffeine.newBuilder()
                .expireAfterWrite(politenessWaitingTime, TimeUnit.SECONDS)
                .build();
    }

    @Override
    public void put(String normalizedUrl) {
        visitedSites.put(normalizedUrl, new Date());
    }

    @Override
    public boolean hasVisited(String normalizedUrl) {
        return visitedSites.getIfPresent(normalizedUrl) != null;
    }

    @Override
    public void close() throws IOException {

    }
}

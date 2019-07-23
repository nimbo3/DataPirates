package in.nimbo.util.cacheManager;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import in.nimbo.util.VisitedSitesCache;

import java.util.Date;
import java.util.concurrent.TimeUnit;

public class CaffeineVistedSiteCache implements VisitedSitesCache {
    private Cache<String, Date> visitedSites = Caffeine.newBuilder()
            .expireAfterWrite(30, TimeUnit.SECONDS)
            .build();

    @Override
    public void put(String normalizedUrl) {
        visitedSites.put(normalizedUrl, new Date());
    }

    @Override
    public boolean hasVisited(String normalizedUrl) {
        return visitedSites.getIfPresent(normalizedUrl) != null;
    }
}

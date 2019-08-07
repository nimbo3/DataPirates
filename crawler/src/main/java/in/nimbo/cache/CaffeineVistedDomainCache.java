package in.nimbo.cache;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.typesafe.config.Config;

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
        SharedMetricRegistries.getDefault().register(
                MetricRegistry.name(CaffeineVistedDomainCache.class, "Caffeine visited links size"),
                (Gauge<Long>) visitedSites::estimatedSize);
    }

    @Override
    public void put(String normalizedUrl) {
        visitedSites.put(normalizedUrl, new Date());
    }

    @Override
    public boolean hasVisited(String normalizedUrl) {
        return visitedSites.getIfPresent(normalizedUrl) != null;
    }

}

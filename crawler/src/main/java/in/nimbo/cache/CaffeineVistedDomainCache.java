package in.nimbo.cache;

import com.codahale.metrics.*;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.typesafe.config.Config;

import java.util.Date;
import java.util.concurrent.TimeUnit;

public class CaffeineVistedDomainCache implements VisitedLinksCache {
    private Timer visitCheckTimer = SharedMetricRegistries.getDefault().timer("caffeine check visit");
    private Meter visitedDomainsFails = SharedMetricRegistries.getDefault().meter("caffeine visited domains fails");
    private Cache<String, Date> visitedSites;
    private int failsCounter = 0;

    public CaffeineVistedDomainCache(Config config) {
        int politenessWaitingTime = config.getInt("politeness.waiting.time");
        visitedSites = Caffeine.newBuilder()
                .expireAfterWrite(politenessWaitingTime, TimeUnit.SECONDS)
                .build();
        SharedMetricRegistries.getDefault().register(
                MetricRegistry.name(CaffeineVistedDomainCache.class, "caffeine visited links"),
                (Gauge<Long>) visitedSites::estimatedSize);
    }

    @Override
    public void put(String url) {
        visitedSites.put(url, new Date());
    }

    @Override
    public boolean hasVisited(String url) {
        try (Timer.Context time = visitCheckTimer.time()) {
            boolean urlVisited = visitedSites.getIfPresent(url) != null;
            if (urlVisited) {
                failsCounter++;
            } else {
                if (failsCounter != 0) {
                    visitedDomainsFails.mark(failsCounter);
                    failsCounter = 0;
                }
            }
            return urlVisited;
        }
    }

}

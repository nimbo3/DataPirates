package in.nimbo;

import com.codahale.metrics.SharedMetricRegistries;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import in.nimbo.cache.CaffeineVistedDomainCache;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CaffeineVisitedDomainCacheTests {

    private static Config config;
    private static Integer politeness;

    @BeforeClass
    public static void initialize () {
        config = ConfigFactory.load("config");
        try {
            SharedMetricRegistries.getDefault();
        } catch (IllegalStateException e) {
            SharedMetricRegistries.setDefault(config.getString("metric.registry.name"));
        }
        politeness = config.getInt("politeness.waiting.time");
    }

    @Test
    public void expireAfterWriteTest () throws InterruptedException {
        CaffeineVistedDomainCache caffeineVistedDomainCache = new CaffeineVistedDomainCache(config);
        caffeineVistedDomainCache.put("domain1.com");
        caffeineVistedDomainCache.put("domain2.com");
        assertTrue(caffeineVistedDomainCache.hasVisited("domain1.com"));
        Thread.sleep(politeness * 1000 + 1000);
        assertFalse(caffeineVistedDomainCache.hasVisited("domain2.com"));
    }
}

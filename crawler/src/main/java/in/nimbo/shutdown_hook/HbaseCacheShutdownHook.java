package in.nimbo.shutdown_hook;

import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import in.nimbo.cache.HBaseVisitedLinksCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HbaseCacheShutdownHook extends Thread {
    private static Logger logger = LoggerFactory.getLogger(HbaseCacheShutdownHook.class);
    private Timer hbaseCacheShutdownTimer = SharedMetricRegistries.getDefault().timer("hbase cache shutdown");
    private HBaseVisitedLinksCache[] hBaseVisitedLinksCaches;


    public HbaseCacheShutdownHook(HBaseVisitedLinksCache[] hBaseVisitedLinksCaches) {
        this.hBaseVisitedLinksCaches = hBaseVisitedLinksCaches;
    }

    @Override
    public void run() {
        try (Timer.Context time = hbaseCacheShutdownTimer.time()) {
            logger.info("Hbase cache Shutdown hook started ...");
            for (HBaseVisitedLinksCache hbaseCacheSiteDao : hBaseVisitedLinksCaches) {
                hbaseCacheSiteDao.close();
            }
            hBaseVisitedLinksCaches[0].closeConnection();
            logger.info("Hbase cache Shutdown hook completed.");
        } catch (IOException e) {
            logger.error("Hbase cache can't close connection!");
        }
    }
}

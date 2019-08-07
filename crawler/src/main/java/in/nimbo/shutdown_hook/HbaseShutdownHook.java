package in.nimbo.shutdown_hook;

import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.typesafe.config.Config;
import in.nimbo.dao.HbaseSiteDaoImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HbaseShutdownHook extends Thread {
    private static Logger logger = LoggerFactory.getLogger(HbaseShutdownHook.class);
    private Timer hbaseShutdownTimer = SharedMetricRegistries.getDefault().timer("hbase shutdown");
    private HbaseSiteDaoImpl[] hbaseSiteDaos;


    public HbaseShutdownHook(HbaseSiteDaoImpl[] hbaseSiteDaos) {
        this.hbaseSiteDaos = hbaseSiteDaos;
    }

    @Override
    public void run() {
        try (Timer.Context time = hbaseShutdownTimer.time()) {
            logger.info("Hbase Shutdown hook started ...");
            for (HbaseSiteDaoImpl hbaseSiteDao : hbaseSiteDaos) {
                hbaseSiteDao.close();
            }
            hbaseSiteDaos[0].closeConnection();
            logger.info("Hbase Shutdown hook completed.");
        } catch (IOException e) {
            logger.error("Hbase can't close connection!");
        }
    }
}

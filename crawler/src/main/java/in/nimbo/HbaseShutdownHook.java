package in.nimbo;

import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.typesafe.config.Config;
import in.nimbo.dao.HbaseSiteDaoImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HbaseShutdownHook extends Thread {
    private static Logger logger = LoggerFactory.getLogger(HbaseShutdownHook.class);
    private final Config config;
    private Timer hbaseShutdownTimer = SharedMetricRegistries.getDefault().timer("hbase shutdown");
    private HbaseSiteDaoImpl[] hbaseSiteDaos;


    public HbaseShutdownHook(HbaseSiteDaoImpl[] hbaseSiteDaos, Config config) {
        this.config = config;
        this.hbaseSiteDaos = hbaseSiteDaos;
    }

    public void run() {
        try (Timer.Context time = hbaseShutdownTimer.time()) {
            for (HbaseSiteDaoImpl hbaseSiteDao : hbaseSiteDaos) {
                hbaseSiteDao.close();
            }
            hbaseSiteDaos[0].closeConnection();
        } catch (IOException e) {
            logger.error("hbase can't close connection!");
        }
    }
}

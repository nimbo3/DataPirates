package in.nimbo.util;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class ZkConfig extends Observable<Config> {
    private static Logger logger = LoggerFactory.getLogger(ZkConfig.class);
    private Config config;
    private ZooKeeper zookeeper;

    public ZkConfig(Config config) throws IOException {
        super(null);
        this.config = config;
        try {
            connect();
            updateConfig();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void connect() throws InterruptedException, IOException {
        CountDownLatch latch = new CountDownLatch(1);
        zookeeper = new ZooKeeper(config.getString("zookeeper.server"), 10000, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                logger.info("Zookeeper connected.");
            } else {
                logger.error("Error connecting to zookeeper");
                throw new RuntimeException("Error connecting to zookeeper");
            }
            latch.countDown();
        });
        latch.await();
    }

    private void updateConfig() {
        try {
            setState(getConfigFromZk());
        } catch (KeeperException e) {
            logger.error("ZooKeeper get exception", e);
        } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }
    }

    private Config getConfigFromZk() throws KeeperException, InterruptedException {
        String configString = new String(zookeeper.getData("/crawler/config",
                event -> updateConfig(), null));
        return ConfigFactory.parseString(configString,
                ConfigParseOptions.defaults().setSyntax(ConfigSyntax.PROPERTIES));
    }

    public Config getConfig() {
        return getState();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
//        ZkConfig zkConfig = new ZkConfig();
//        zkConfig.addObserver(newState -> System.out.println(newState.getString("queue.link.pair.html.size")));
        Thread.sleep(60000);
    }
}

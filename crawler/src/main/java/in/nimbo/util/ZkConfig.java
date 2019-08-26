package in.nimbo.util;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class ZkConfig extends Observable<Config> {
    private static Logger logger = LoggerFactory.getLogger(ZkConfig.class);
    private Map<String, String> previousConfigMap = new HashMap<>();
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
                System.exit(-1);
            }
            latch.countDown();
        });
        latch.await();
    }

    private void updateConfig() {
        try {
            Map<String, String> newConfig = getConfigMapFromZk();
            if (!newConfig.equals(previousConfigMap)) {
                previousConfigMap = newConfig;
                setState(ConfigFactory.parseMap(newConfig));
            }
        } catch (InterruptedException e) {
            logger.error("interrupted!", e);
            Thread.currentThread().interrupt();
        } catch (KeeperException e) {
            logger.error("couldn't load initial config from zookeeper", e);
            System.exit(-1);
        }
    }

    private Map<String, String> getConfigMapFromZk() throws KeeperException, InterruptedException {
        List<String> children = zookeeper.getChildren("/crawler", event -> updateConfig());
        Map<String, String> configMap = getConfigMapFromPath("/crawler");
        try {
            String hostName = InetAddress.getLocalHost().getHostName();
            if (children.contains(hostName))
                configMap.putAll(getConfigMapFromPath("/crawler/" + hostName));
        } catch (UnknownHostException e) {
            logger.error("can't get machine name", e);
        }
        return configMap;
    }

    private Map<String, String> getConfigMapFromPath(String path) throws KeeperException, InterruptedException {
        if (!zookeeper.getChildren(path, event -> updateConfig()).contains("config"))
            return new HashMap<>();
        List<String> configList = zookeeper.getChildren(path + "/config", event -> updateConfig());
        Map<String, String> configMap = new HashMap<>();
        configList.forEach(configName -> {
            try {
                String configValue = new String(zookeeper.getData(path + "/config/" + configName,
                        event -> updateConfig(), null));
                configMap.put(configName, configValue);
            } catch (KeeperException e) {
                logger.error("can't get config from zookeeper", e);
                System.exit(-1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        return configMap;
    }

    public Config getConfig() {
        return getState();
    }

    /**
     * path should exist in zookeeper
     */
    public void initInZooKeeper(String resource, String path) {
        ConfigFactory.parseResourcesAnySyntax(resource).entrySet().forEach(ConfigEntry -> {
            try {
                updateConfigNode(path, ConfigEntry);
            } catch (KeeperException e) {
                if (e.code() == KeeperException.Code.NONODE) {
                    createConfigNode(path, ConfigEntry);
                } else {
                    logger.error("exception in writing on zookeeper", e);
                    System.exit(-1);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    private void createConfigNode(String path, Map.Entry<String, ConfigValue> configEntry) {
        try {
            zookeeper.create(path + "/config/" + configEntry.getKey(),
                    ((String) configEntry.getValue().unwrapped()).getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException e) {
            logger.error("couldn't write on zookeeper", e);
            System.exit(-1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void updateConfigNode(String path, Map.Entry<String, ConfigValue> configEntry)
            throws KeeperException, InterruptedException {
        Stat stat = new Stat();
        zookeeper.getData(path + "/config/" + configEntry.getKey(), null, stat);
        zookeeper.setData(path + "/config/" + configEntry.getKey(),
                ((String) configEntry.getValue().unwrapped()).getBytes(), stat.getVersion());
    }

    public static void main(String[] args) throws IOException {
        ZkConfig zkConfig = new ZkConfig(ConfigFactory.parseResourcesAnySyntax("config"));
        zkConfig.initInZooKeeper("config", "/crawler");
    }
}

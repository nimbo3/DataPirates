package in.nimbo.cache;

import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.typesafe.config.Config;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;

import java.util.ArrayList;

public class RedisVisitedLinksCache implements VisitedLinksCache {
    private final Config config;
    private final RedisAdvancedClusterCommands<String, String> sync;
    private Timer visitingCheckTimer = SharedMetricRegistries.getDefault().timer("redis-visited-check");
    private RedisClusterClient redisClusterClient;
    private StatefulRedisClusterConnection<String, String> connection;

    public RedisVisitedLinksCache(Config config) {
        this.config = config;
        ArrayList<RedisURI> redisServers = new ArrayList<>();
        for (String string : config.getString("redis.servers").split(","))
            redisServers.add(RedisURI.create("redis://"+string));
        redisClusterClient = RedisClusterClient.create(redisServers);
        connection = redisClusterClient.connect();
        sync = connection.sync();
    }

    @Override
    public void put(String normalizedUrl) {
        sync.set(normalizedUrl, "");
    }

    @Override
    public boolean hasVisited(String normalizedUrl) {
        try (Timer.Context time = visitingCheckTimer.time()) {
            return sync.get(normalizedUrl) != null;
        }
    }

    @Override
    public void close() {
        connection.close();
        redisClusterClient.shutdown();
    }
}

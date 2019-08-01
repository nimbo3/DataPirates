package in.nimbo.cache;

import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.typesafe.config.Config;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;

import java.util.ArrayList;

public class RedisVisitedLinksCache implements VisitedLinksCache {
    private final Config config;
    private RedisAdvancedClusterCommands<String, String> sync;
    private RedisAdvancedClusterAsyncCommands<String, String> async;
    private Timer visitingCheckTimer;
    private RedisClusterClient redisClusterClient;
    private StatefulRedisClusterConnection<String, String> connection;

    public RedisVisitedLinksCache(Config config) {
        this.config = config;
        visitingCheckTimer = SharedMetricRegistries.getDefault().timer("metric.name.redis.visit.check");
        ArrayList<RedisURI> redisServers = new ArrayList<>();
        for (String string : config.getString("redis.servers").split(","))
            redisServers.add(RedisURI.create("redis://"+string));
        redisClusterClient = RedisClusterClient.create(redisServers);
        connection = redisClusterClient.connect();
        async = connection.async();
        sync = connection.sync();
    }

    @Override
    public void put(String normalizedUrl) {
        async.set(normalizedUrl, "");
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

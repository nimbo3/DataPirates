package in.nimbo.util;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;

import java.io.Closeable;
import java.util.ArrayList;

public class RedisVisitedLinksCache implements VisitedLinksCache, Closeable {
    private RedisClusterClient redisClusterClient;
    private StatefulRedisClusterConnection<String, String> connection;
    private RedisAdvancedClusterAsyncCommands<String, String> async;
    private final RedisAdvancedClusterCommands<String, String> sync;

    public RedisVisitedLinksCache(Config config) {
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
        return sync.get(normalizedUrl) != null;
    }

    @Override
    public void close() {
        connection.close();
        redisClusterClient.shutdown();
    }
}

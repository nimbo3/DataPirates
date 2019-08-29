package in.nimbo.dao;

import com.typesafe.config.Config;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;

import java.io.Closeable;
import java.util.ArrayList;

public class RedisDao implements Closeable {
    private RedisAdvancedClusterAsyncCommands<String, String> async;
    private StatefulRedisClusterConnection<String, String> connection;
    private RedisClusterClient redisClusterClient;

    public RedisDao(Config config) {
        ArrayList<RedisURI> redisServers = new ArrayList<>();
        for (String string : config.getString("redis.servers").split(","))
            redisServers.add(RedisURI.create("redis://" + string));
        redisClusterClient = RedisClusterClient.create(redisServers);
        connection = redisClusterClient.connect();
        async = connection.async();
    }

    public void put(String normalizedUrl) {
        async.set(normalizedUrl, "");
    }

    @Override
    public void close() {
        connection.close();
        redisClusterClient.shutdown();
    }
}

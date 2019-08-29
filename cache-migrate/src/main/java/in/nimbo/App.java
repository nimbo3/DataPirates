package in.nimbo;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.jmx.JmxReporter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import in.nimbo.dao.HbaseCacheDao;
import in.nimbo.dao.RedisDao;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class App
{
    public static void main( String[] args ) throws IOException {
        SharedMetricRegistries.setDefault("data-pirates-crawler");
        final MetricRegistry metricRegistry = SharedMetricRegistries.getDefault();
        JmxReporter jmxReporter = JmxReporter.forRegistry(metricRegistry).inDomain("cache migrate").build();
        jmxReporter.start();
        Configuration hbaseConfig = HBaseConfiguration.create();
        final Connection hbaseConnection = ConnectionFactory.createConnection(hbaseConfig);
        Config config = ConfigFactory.load("config");
        HbaseCacheDao hbaseCacheDao = new HbaseCacheDao(hbaseConnection, config);
        RedisDao redisDao = new RedisDao(config);
        hbaseCacheDao.scan(redisDao);
    }
}

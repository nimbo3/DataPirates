package in.nimbo;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.util.List;

public class App
{
    public static void main( String[] args ) throws IOException {
        Configuration hbaseConfig = HBaseConfiguration.create();
        final Connection hbaseConnection = ConnectionFactory.createConnection(hbaseConfig);
        Config config = ConfigFactory.load("config");
        HbaseCacheDao hbaseCacheDao = new HbaseCacheDao(hbaseConnection, config);
        final List<String> scan = hbaseCacheDao.scan();
    }
}

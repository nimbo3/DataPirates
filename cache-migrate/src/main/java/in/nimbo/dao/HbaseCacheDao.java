package in.nimbo.dao;

import com.codahale.metrics.Meter;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.typesafe.config.Config;
import in.nimbo.util.HashCodeGenerator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HbaseCacheDao {
    private static final Logger logger = LoggerFactory.getLogger(HbaseCacheDao.class);
    private final String CACHE_TABLE_NAME;
    private Connection connection;
    private String FLAG_FAMILY;
    private Timer hbaseScanTimer;
    private Meter hbaseScanMeter;


    public HbaseCacheDao(Connection connection, Config config) {
        this.connection = connection;
        CACHE_TABLE_NAME = config.getString("hbase.cache.table.name");
        FLAG_FAMILY = config.getString("hbase.cache.table.column.family");
        hbaseScanTimer = SharedMetricRegistries.getDefault().timer("hbase-scan-timer");
        hbaseScanMeter = SharedMetricRegistries.getDefault().meter("hbase-scan-meter");
    }

    public void scan(RedisDao redisDao) {
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(FLAG_FAMILY));
        scan.setFilter(new FirstKeyOnlyFilter());
        try (Timer.Context time = hbaseScanTimer.time();
             Table table = connection.getTable(TableName.valueOf(CACHE_TABLE_NAME));
             ResultScanner scanner = table.getScanner(scan)) {
            for (Result result = scanner.next(); result != null; result = scanner.next()) {
                redisDao.put(HashCodeGenerator.md5HashString(Bytes.toString(result.getRow())));
                hbaseScanMeter.mark();
            }
        } catch (IOException e) {
            logger.error("can't connect to table with name: " + CACHE_TABLE_NAME);
        }
    }
}

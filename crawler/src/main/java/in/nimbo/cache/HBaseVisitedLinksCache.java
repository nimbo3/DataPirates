package in.nimbo.cache;

import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.typesafe.config.Config;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class HBaseVisitedLinksCache extends Thread implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(HBaseVisitedLinksCache.class);
    private final String CACHE_TABLE_NAME;
    private final int BULK_SIZE;
    private Timer hbaseBulkCacheInsertMeter = SharedMetricRegistries.getDefault().timer("hbase-bulk-cache-insert");
    private LinkedBlockingQueue<String> linkCache;
    private List<Put> putsCache = new LinkedList<>();
    private Connection connection;
    private String FLAG_FAMILY;
    private boolean closed = false;


    public HBaseVisitedLinksCache(Connection connection, LinkedBlockingQueue<String> linkCache, Config config) {
        this.connection = connection;
        CACHE_TABLE_NAME = config.getString("hbase.cache.table.name");
        FLAG_FAMILY = config.getString("hbase.cache.table.column.family");
        BULK_SIZE = config.getInt("hbase.bulk.size");
        this.linkCache = linkCache;
    }

    @Override
    public void run() {
        try {
            while (!closed && !interrupted()) {
                Put putCache = new Put(Bytes.toBytes(linkCache.take()));
                putCache.addColumn(Bytes.toBytes(FLAG_FAMILY),
                        Bytes.toBytes(FLAG_FAMILY), Bytes.toBytes(true));
                putsCache.add(putCache);

                if (putsCache.size() >= BULK_SIZE) {
                    try (Table table = connection.getTable(TableName.valueOf(CACHE_TABLE_NAME));
                         Timer.Context time = hbaseBulkCacheInsertMeter.time()) {
                        table.put(putsCache);
                    } catch (IOException e) {
                        logger.error("Hbase thread can't bulk insert cache files.", e);
                    }
                    putsCache.clear();
                }
            }
        } catch (InterruptedException e) {
            logger.error("hbase-bulk-insertion thread interrupted!");
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        closed = true;
        if (!putsCache.isEmpty()) {
            try (Table table = connection.getTable(TableName.valueOf(CACHE_TABLE_NAME));
                 Timer.Context time = hbaseBulkCacheInsertMeter.time()) {
                table.put(putsCache);
            } catch (IOException e) {
                logger.error("Hbase thread can't bulk insert cache files.", e);
            }
            putsCache.clear();
        }
    }

    public void closeConnection() throws IOException {
        connection.close();
    }
}

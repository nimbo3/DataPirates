package in.nimbo.dao;

import com.codahale.metrics.Meter;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.typesafe.config.Config;
import in.nimbo.exception.HbaseSiteDaoException;
import in.nimbo.exception.SiteDaoException;
import in.nimbo.model.Site;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class HbaseSiteDaoImpl extends Thread implements Closeable, SiteDao {
    private static final Logger logger = LoggerFactory.getLogger(HbaseSiteDaoImpl.class);
    private final String TABLE_NAME;
    private final String CACHE_TABLE_NAME;
    private final int BULK_SIZE;
    private Timer hbaseBulkInsertMeter = SharedMetricRegistries.getDefault().timer("hbase-bulk-insert");
    private Timer hbaseBulkCacheInsertMeter = SharedMetricRegistries.getDefault().timer("hbase-bulk-cache-insert");
    private Timer insertionTimer = SharedMetricRegistries.getDefault().timer("hbase-insertion");
    private Meter insertionFailureMeter = SharedMetricRegistries.getDefault().meter("hbase-insertion-failure");
    private Timer deleteTimer = SharedMetricRegistries.getDefault().timer("hbase-delete");
    private LinkedBlockingQueue<Site> sites;
    private LinkedBlockingQueue<String> linkCache;
    private List<Put> puts = new LinkedList<>();
    private List<Put> putsCache = new LinkedList<>();
    private Connection connection;
    private String ANCHORS_FAMILY;
    private String FLAG_FAMILY;
    private boolean closed = false;

    public HbaseSiteDaoImpl(Connection connection, LinkedBlockingQueue<Site> sites, LinkedBlockingQueue<String> linkCache, Config config) {
        this.connection = connection;
        TABLE_NAME = config.getString("hbase.table.name");
        CACHE_TABLE_NAME = config.getString("hbase.cache.table.name");
        ANCHORS_FAMILY = config.getString("hbase.table.column.family.anchors");
        FLAG_FAMILY = config.getString("hbase.cache.table.column.family");
        BULK_SIZE = config.getInt("hbase.bulk.size");
        this.sites = sites;
        this.linkCache = linkCache;
    }

    public HbaseSiteDaoImpl(Connection connection, Config config) {
        TABLE_NAME = config.getString("hbase.table.name");
        CACHE_TABLE_NAME = config.getString("hbase.cache.table.name");
        ANCHORS_FAMILY = config.getString("hbase.table.column.family.anchors");
        FLAG_FAMILY = config.getString("hbase.cache.table.column.family");
        BULK_SIZE = config.getInt("hbase.bulk.size");
        this.connection = connection;
    }

    @Override
    public void insert(Site site) throws SiteDaoException {
        try (Timer.Context time = insertionTimer.time()) {
            try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
                Put put = new Put(Bytes.toBytes(site.getNoProtocolLink()));
                for (Map.Entry<String, String> anchorEntry : site.getNoProtocolAnchors().entrySet()) {
                    String link = anchorEntry.getKey();
                    String text = anchorEntry.getValue();
                    put.addColumn(Bytes.toBytes(ANCHORS_FAMILY),
                            Bytes.toBytes(link), Bytes.toBytes(text));
                }
                table.put(put);
            }
        } catch (IOException | IllegalArgumentException e) {
            insertionFailureMeter.mark();
            throw new HbaseSiteDaoException("Hbase can't insert: " + site.getLink(), e);
        }
    }

    @Override
    public void delete(Site site) {
        try {
            String link = site.getNoProtocolLink();
            try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
                 Timer.Context time = deleteTimer.time()) {
                Delete del = new Delete(Bytes.toBytes(link));
                table.delete(del);
                logger.debug(String.format("Link [%s] deleted from hbase", link));
            }
        } catch (IOException e) {
            logger.error("can't delete this link: " + site.getLink() + "from hbase", e);
        }
    }

    public Result get(String link) throws SiteDaoException {
        try {
            try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
                 Timer.Context time = deleteTimer.time()) {
                Get get = new Get(Bytes.toBytes(link));
                return table.get(get);
            }
        } catch (IOException e) {
            throw new HbaseSiteDaoException("can't get from Hbase", e);
        }
    }

    public boolean contains(Site site) throws SiteDaoException {
        try {
            try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
                 Timer.Context time = deleteTimer.time()) {
                Get get = new Get(Bytes.toBytes(site.getNoProtocolLink()));
                Result result = table.get(get);
                return result.size() > 0;
            }
        } catch (IOException e) {
            throw new HbaseSiteDaoException(e);
        }
    }

    @Override
    public void run() {
        try {
            while (!closed && !interrupted()) {
                Put putCache = new Put(Bytes.toBytes(linkCache.take()));
                putCache.addColumn(Bytes.toBytes(FLAG_FAMILY),
                        Bytes.toBytes(FLAG_FAMILY), Bytes.toBytes(true));
                putsCache.add(putCache);
                Site site = sites.take();
                Put put = new Put(Bytes.toBytes(site.getNoProtocolLink()));
                for (Map.Entry<String, String> anchorEntry : site.getNoProtocolAnchors().entrySet()) {
                    String link = anchorEntry.getKey();
                    String text = anchorEntry.getValue();
                    put.addColumn(Bytes.toBytes(ANCHORS_FAMILY),
                            Bytes.toBytes(link), Bytes.toBytes(text));
                }
                puts.add(put);
                if (puts.size() >= BULK_SIZE) {
                    try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
                         Timer.Context time = hbaseBulkInsertMeter.time()) {
                        table.put(puts);
                    } catch (IOException e) {
                        logger.error("Hbase thread can't bulk insert.", e);
                    }
                    puts.clear();
                }
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
        if (!puts.isEmpty()) {
            try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
                 Timer.Context time = hbaseBulkInsertMeter.time()) {
                table.put(puts);
            } catch (IOException e) {
                logger.error("Hbase thread can't bulk insert.", e);
            }
            puts.clear();
        }
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

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
    private final int BULK_SIZE;
    private Timer hbaseBulkInsertMeter = SharedMetricRegistries.getDefault().timer("hbase-bulk-insert");
    private Timer insertionTimer = SharedMetricRegistries.getDefault().timer("hbase-insertion");
    private Meter insertionFailureMeter = SharedMetricRegistries.getDefault().meter("hbase-insertion-failure");
    private Timer deleteTimer = SharedMetricRegistries.getDefault().timer("hbase-delete");
    private Timer getMethodTimer = SharedMetricRegistries.getDefault().timer("hbase-get");
    private Timer containsTimer = SharedMetricRegistries.getDefault().timer("hbase-contains");
    private Meter deleteFailureMeter = SharedMetricRegistries.getDefault().meter("hbase-delete-failure");
    private LinkedBlockingQueue<Site> sites;
    private List<Put> puts = new LinkedList<>();
    private Connection connection;
    private String ANCHORS_FAMILY;
    private boolean closed = false;


    public HbaseSiteDaoImpl(Connection connection, LinkedBlockingQueue<Site> sites, Config config) {
        this.connection = connection;
        TABLE_NAME = config.getString("hbase.table.name");
        ANCHORS_FAMILY = config.getString("hbase.table.column.family.anchors");
        BULK_SIZE = config.getInt("hbase.bulk.size");
        this.sites = sites;
    }

    public HbaseSiteDaoImpl(Connection connection, Config config) {
        TABLE_NAME = config.getString("hbase.table.name");
        ANCHORS_FAMILY = config.getString("hbase.table.column.family.anchors");
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
    public void delete(Site site) throws HbaseSiteDaoException {
        try {
            String noProtocolLink = site.getNoProtocolLink();
            try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
                 Timer.Context time = deleteTimer.time()) {
                Delete del = new Delete(Bytes.toBytes(noProtocolLink));
                table.delete(del);
            }
        } catch (IOException e) {
            deleteFailureMeter.mark();
            throw new HbaseSiteDaoException("can't delete this link: " + site.getLink() + "from hbase", e);
        }
    }

    public Result get(String noProtocolLink) throws SiteDaoException {
        try {
            try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
                 Timer.Context time = getMethodTimer.time()) {
                Get get = new Get(Bytes.toBytes(noProtocolLink));
                return table.get(get);
            }
        } catch (IOException e) {
            throw new HbaseSiteDaoException("can't get link: " + noProtocolLink + "from Hbase", e);
        }
    }

    public boolean contains(Site site) throws SiteDaoException {
        try {
            try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
                 Timer.Context time = containsTimer.time()) {
                Get get = new Get(Bytes.toBytes(site.getNoProtocolLink()));
                Result result = table.get(get);
                return result.size() > 0;
            }
        } catch (IOException e) {
            throw new HbaseSiteDaoException("can't perform contains for: " + site.getLink() + "on Hbase", e);
        }
    }

    @Override
    public void run() {
        try {
            while (!closed && !interrupted()) {
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
    }

    public void closeConnection() throws IOException {
        connection.close();
    }
}

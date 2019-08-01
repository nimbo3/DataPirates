package in.nimbo.dao;

import com.codahale.metrics.Meter;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.typesafe.config.Config;
import in.nimbo.exception.HbaseSiteDaoException;
import in.nimbo.exception.SiteDaoException;
import in.nimbo.model.Site;
import org.apache.hadoop.conf.Configuration;
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
    private Config config;
    private Timer hbasebulkInsertMeter = SharedMetricRegistries.getDefault().timer("hbase-bulk-insert");
    private Timer insertionTimer = SharedMetricRegistries.getDefault().timer("hbase-insertion");
    private Meter insertionFailureMeter = SharedMetricRegistries.getDefault().meter("hbase-insertion-failure");
    private Timer deleteTimer = SharedMetricRegistries.getDefault().timer("hbase-delete");
    private LinkedBlockingQueue<Site> sites;
    private List<Put> puts = new LinkedList<>();
    private Connection conn;
    private Configuration hbaseConfig;
    private String anchorsFamily;
    private boolean closed = false;


    public HbaseSiteDaoImpl(Connection conn, LinkedBlockingQueue<Site> sites, Configuration hbaseConfig, Config config) {
        this.config = config;
        this.conn = conn;
        this.hbaseConfig = hbaseConfig;
        TABLE_NAME = config.getString("hbase.table.name");
        anchorsFamily = config.getString("hbase.table.column.family.anchors");
        BULK_SIZE = config.getInt("hbase.bulk.size");
        this.sites = sites;
    }
    public HbaseSiteDaoImpl(Configuration hbaseConfig, Config config) throws HbaseSiteDaoException {
        this.config = config;
        this.hbaseConfig = hbaseConfig;
        TABLE_NAME = config.getString("hbase.table.name");
        anchorsFamily = config.getString("hbase.table.column.family.anchors");
        BULK_SIZE = config.getInt("hbase.bulk.size");
        try {
            getConnection();
        } catch (IOException e) {
            throw new HbaseSiteDaoException("can't connect to hbase!", e);
        }
    }

    private Connection getConnection() throws IOException {
        if (conn == null)
            conn = ConnectionFactory.createConnection(hbaseConfig);
        return conn;
    }

    @Override
    public void insert(Site site) throws SiteDaoException {
        try {
            Connection connection = getConnection();
            try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
                 Timer.Context time = insertionTimer.time()) {
                Put put = new Put(Bytes.toBytes(site.getReverseLink()));
                for (Map.Entry<String, String> anchorEntry : site.getAnchors().entrySet()) {
                    String link = anchorEntry.getKey();
                    String text = anchorEntry.getValue();
                    put.addColumn(Bytes.toBytes(anchorsFamily),
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
    public void delete(String reverseLink) {
        try {
            Connection connection = getConnection();
            try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
                 Timer.Context time = deleteTimer.time()) {
                Delete del = new Delete(Bytes.toBytes(reverseLink));
                table.delete(del);
                logger.debug(String.format("Link [%s] deleted from hbase", reverseLink));
            }
        } catch (IOException e) {
            logger.error("can't delete this link: " + reverseLink + "from hbase", e);
        }
    }

    public Result get(String reverseLink) throws SiteDaoException {
        try {
            Connection connection = getConnection();
            try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
                 Timer.Context time = deleteTimer.time()) {
                Get get = new Get(Bytes.toBytes(reverseLink));
                return table.get(get);
            }
        } catch (IOException e) {
            throw new HbaseSiteDaoException("can't bulk get from Hbase", e);
        }
    }

    public boolean contains(Site site) throws SiteDaoException {
        try {
            Connection connection = getConnection();
            try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
                 Timer.Context time = deleteTimer.time()) {
                Get get = new Get(Bytes.toBytes(site.getLink()));
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
                Site site = sites.take();
                Put put = new Put(Bytes.toBytes(site.getReverseLink()));
                for (Map.Entry<String, String> anchorEntry : site.getAnchors().entrySet()) {
                    String link = anchorEntry.getKey();
                    String text = anchorEntry.getValue();
                    put.addColumn(Bytes.toBytes(anchorsFamily),
                            Bytes.toBytes(link), Bytes.toBytes(text));
                }
                puts.add(put);
                if (puts.size() >= BULK_SIZE) {
                    try (Table table = getConnection().getTable(TableName.valueOf(TABLE_NAME));
                         Timer.Context time = hbasebulkInsertMeter.time()) {
                        table.put(puts);
                    } catch (IOException e) {
                       logger.error("Hbase thread can't bulk insert.", e);
                    }
                    puts.clear();
                }
            }
        } catch (InterruptedException e) {
            logger.error("hbase-bulk-insertion thread interrupted!");
        }
    }

    @Override
    public void close() {
        closed = true;
        if(!puts.isEmpty()){
            try (Table table = getConnection().getTable(TableName.valueOf(TABLE_NAME));
                 Timer.Context time = hbasebulkInsertMeter.time()) {
                table.put(puts);
            } catch (IOException e) {
                logger.error("Hbase thread can't bulk insert.", e);
            }
            puts.clear();
        }
    }

    public void closeConnection() throws IOException {
        conn.close();
    }
}

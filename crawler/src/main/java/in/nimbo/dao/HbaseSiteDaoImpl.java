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

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


public class HbaseSiteDaoImpl implements SiteDao {
    private static final Logger logger = LoggerFactory.getLogger(SiteDao.class);
    private final Configuration hbaseConfig;
    private final String TABLE_NAME;
    private final Config config;
    private Timer insertionTimer = SharedMetricRegistries.getDefault().timer("hbase-insertion");
    private Meter insertionFailureMeter = SharedMetricRegistries.getDefault().meter("hbase-insertion-failure");
    private Timer deleteTimer = SharedMetricRegistries.getDefault().timer("hbase-delete");
    private String family1;
    private Connection conn;
    private List<Put> addBulk = new LinkedList<>();
    private List<Get> getBulk = new LinkedList<>();

    public HbaseSiteDaoImpl(Configuration hbaseConfig, Config config) throws HbaseSiteDaoException {
        TABLE_NAME = config.getString("hbase.table.name");
        family1 = config.getString("hbase.table.column.family.anchors");
        this.hbaseConfig = hbaseConfig;
        this.config = config;
        try {
            getConnection();
            logger.info("connection available to hbase!");
        } catch (IOException e) {
            throw new HbaseSiteDaoException("connection not available to hbase!", e);
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
                    put.addColumn(Bytes.toBytes(family1),
                            Bytes.toBytes(link), Bytes.toBytes(text));
                    addBulk.add(put);
                }
                if (addBulk.size() > config.getInt("hbase.bulk.size")) {
                    table.put(addBulk);
                    addBulk.clear();
                }
            }
        } catch (IOException | IllegalArgumentException e) {
            insertionFailureMeter.mark();
            throw new HbaseSiteDaoException("Hbase can't bulk insert: " + site.getReverseLink(), e);
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
            }
        } catch (IOException e) {
            logger.error("can't delete this link: " + reverseLink + "from hbase", e);
        }
    }

    public Result[] get(String reverseLink) throws SiteDaoException {
        try {
            Connection connection = getConnection();
            try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
                 Timer.Context time = deleteTimer.time()) {
                Get get = new Get(Bytes.toBytes(reverseLink));
                getBulk.add(get);
                Result[] results = null;
                if (getBulk.size() > config.getInt("hbase.bulk.size")) {
                    results = table.get(getBulk);
                }
                return results;
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
}

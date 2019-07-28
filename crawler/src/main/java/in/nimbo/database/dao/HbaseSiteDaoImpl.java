package in.nimbo.database.dao;

import com.codahale.metrics.Meter;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.typesafe.config.Config;
import in.nimbo.exception.SiteDaoException;
import in.nimbo.model.Site;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;


public class HbaseSiteDaoImpl implements SiteDao {
    private static final Logger logger = LoggerFactory.getLogger(SiteDao.class);
    private Timer insertionTimer = SharedMetricRegistries.getDefault().timer("hbase-insertion");
    private Meter insertionFailureMeter = SharedMetricRegistries.getDefault().meter("hbase-insertion-failure");
    private final Configuration hbaseConfig;
    private final String TABLE_NAME;
    private final Config config;
    private String family1;
    private Connection conn;

    public HbaseSiteDaoImpl(Configuration hbaseConfig, Config config) {
        TABLE_NAME = config.getString("hbase.table.name");
        family1 = config.getString("hbase.table.column.family.anchors");
        this.hbaseConfig = hbaseConfig;
        this.config = config;
        try {
            getConnection();
            logger.info("connection available to hbase!");
        } catch (IOException e) {
            logger.error("connection not available to hbase :(((");
        }
    }

    private Connection getConnection() throws IOException {
        if (conn == null)
            conn = ConnectionFactory.createConnection(hbaseConfig);
        return conn;
    }


    @Override
    public void insert(Site site) throws SiteDaoException {
        try (Connection connection = ConnectionFactory.createConnection(hbaseConfig);
             Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
             Timer.Context time = insertionTimer.time()) {

            Put put = new Put(Bytes.toBytes(site.getLink()));
            for (String qualifier : site.getAnchors().keySet()) {
                String value = site.getAnchors().get(qualifier);
                put.addColumn(Bytes.toBytes(family1),
                        Bytes.toBytes(qualifier), Bytes.toBytes(value));
            }
            table.put(put);
        } catch (IOException | IllegalArgumentException e) {
            logger.error(String.format("Hbase couldn't insert [%s]", site.getLink()), e);
            insertionFailureMeter.mark();
            throw new SiteDaoException(e);
        }
    }

    @Override
    public void delete(String url) {

    }

    public Map<byte[], byte[]> get(Site site) throws SiteDaoException {
        try (Table table = getConnection().getTable(TableName.valueOf(TABLE_NAME))) {
            Get get = new Get(Bytes.toBytes(site.getLink()));
            Result result = table.get(get);
            return result.getFamilyMap(Bytes.toBytes(family1));
        } catch (IOException e) {
            throw new SiteDaoException(e);
        }
    }

    public boolean contains(Site site) throws SiteDaoException {
        try (Connection connection = ConnectionFactory.createConnection(hbaseConfig);
             Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Get get = new Get(Bytes.toBytes(site.getLink()));
            Result result = table.get(get);
            return result.size() > 0;
        } catch (IOException e) {
            throw new SiteDaoException(e);
        }
    }
}

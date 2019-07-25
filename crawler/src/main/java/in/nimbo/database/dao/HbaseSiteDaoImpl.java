package in.nimbo.database.dao;

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
    private final Configuration hbaseConfig;
    private final String TABLE_NAME;
    private final Config config;
    private String family1;

    public HbaseSiteDaoImpl(Configuration hbaseConfig, Config config) throws SiteDaoException {
        TABLE_NAME = config.getString("hbase.table.name");
        family1 = config.getString("hbase.table.column.family.anchors");
        this.hbaseConfig = hbaseConfig;
        this.config = config;
        try {
            HBaseAdmin.available(hbaseConfig);
            logger.info("connection available to hbase!");
        } catch (IOException e) {
            throw new SiteDaoException(e);
        }
    }


    @Override
    public void insert(Site site) throws SiteDaoException {
        try (Connection connection = ConnectionFactory.createConnection(hbaseConfig)) {
            Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
            Put put = new Put(Bytes.toBytes(site.getLink()));
            for (String qualifier : site.getAnchors().keySet()) {
                String value = site.getAnchors().get(qualifier);
                put.addColumn(Bytes.toBytes(family1),
                        Bytes.toBytes(qualifier), Bytes.toBytes(value));
            }
            table.put(put);
        } catch (IOException e) {
            throw new SiteDaoException(e);
        }
    }

    public Map<byte[], byte[]> get(Site site) throws SiteDaoException {
        try (Connection connection = ConnectionFactory.createConnection(hbaseConfig)) {
            Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
            Get get = new Get(Bytes.toBytes(site.getLink()));
            Result result = table.get(get);
            return result.getFamilyMap(Bytes.toBytes(family1));
        } catch (IOException e) {
            throw new SiteDaoException(e);
        }
    }

    public boolean contains(Site site) throws SiteDaoException {
        try (Connection connection = ConnectionFactory.createConnection(hbaseConfig)) {
            Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
            Get get = new Get(Bytes.toBytes(site.getLink()));
            Result result = table.get(get);
            return result.size() > 0;
        } catch (IOException e) {
            throw new SiteDaoException(e);
        }
    }

    public void create() throws SiteDaoException {
        try (Connection connection = ConnectionFactory.createConnection(hbaseConfig)) {
            Admin admin = connection.getAdmin();
            TableDescriptor desc = TableDescriptorBuilder.newBuilder(TableName.valueOf(TABLE_NAME)).setColumnFamily(
                    ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family1)).build()
            ).build();
            admin.createTable(desc);
        } catch (IOException e) {
            throw new SiteDaoException(e);
        }
    }
}

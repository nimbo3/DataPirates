package in.nimbo.dao;


import com.codahale.metrics.SharedMetricRegistries;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import in.nimbo.exception.HbaseSiteDaoException;
import in.nimbo.exception.SiteDaoException;
import in.nimbo.model.Site;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;

import static org.junit.Assert.assertEquals;


public class HbaseSiteDaoImplTest {
    private static String TABLE_NAME;
    private static String FAMILY_NAME;
    private static HbaseSiteDaoImpl hbaseSiteDao;
    private static Connection conn;

    @BeforeClass
    public static void init() throws HbaseSiteDaoException, IOException {
        Config config = ConfigFactory.load("config");
        try {
            SharedMetricRegistries.getDefault();
        } catch (IllegalStateException e) {
            SharedMetricRegistries.setDefault(config.getString("metric.registry.name"));
        }
        Configuration hBaseConfiguration = HBaseConfiguration.create();
        conn = ConnectionFactory.createConnection(hBaseConfiguration);
        hbaseSiteDao = new HbaseSiteDaoImpl(conn, hBaseConfiguration, config);
        TABLE_NAME = config.getString("hbase.table.name");
        FAMILY_NAME = config.getString("hbase.table.column.family.anchors");
        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
        desc.addFamily(new HColumnDescriptor(FAMILY_NAME));
        conn.getAdmin().createTable(desc);
    }

    @After
    public void truncateTable() throws IOException {
        final Admin admin = conn.getAdmin();
        admin.disableTable(TableName.valueOf(TABLE_NAME));
        admin.truncateTable(TableName.valueOf(TABLE_NAME), false);
    }

    @Test
    public void insert() {
        Site site = new Site("www.google.com", "welcome to google!");
        Map<String, String> map = new HashMap<>();
        map.put("www.stackoverflow.com/google", "see stack");
        map.put("www.yahoo.com/google", "see yahoo");
        site.setAnchors(map);
        site.setReverseLink("com.google.www");
        try {
            hbaseSiteDao.insert(site);
            NavigableMap<byte[], byte[]> actualByteMap = hbaseSiteDao.get(site.getReverseLink()).getFamilyMap(Bytes.toBytes("links"));
            Map<String, String> actual = new HashMap<>();
            for (Map.Entry<byte[], byte[]> entry : actualByteMap.entrySet()) {
                actual.put(new String(entry.getKey()), new String(entry.getValue()));
            }
            assertEquals(site.getAnchors(), actual);
        } catch (Exception e) {
            System.out.println("exception thrown can't insert");
            e.printStackTrace();
        }
    }

    @Test
    public void delete() {
    }

    @Test
    public void get() {
    }

    @Test
    public void contains() {
    }
}

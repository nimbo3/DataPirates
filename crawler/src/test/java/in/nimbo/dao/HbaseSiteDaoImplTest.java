package in.nimbo.dao;


import com.codahale.metrics.SharedMetricRegistries;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import in.nimbo.exception.SiteDaoException;
import in.nimbo.model.Site;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.MalformedURLException;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;

import static org.junit.Assert.*;


public class HbaseSiteDaoImplTest {
    private static String TABLE_NAME;
    private static String FAMILY_NAME;
    private static HbaseSiteDaoImpl hbaseSiteDao;
    private static Connection conn;

    @BeforeClass
    public static void init() throws IOException {
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
        try (final Admin admin = conn.getAdmin()) {
            if (!admin.tableExists(TableName.valueOf(TABLE_NAME))) {
                HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
                desc.addFamily(new HColumnDescriptor(FAMILY_NAME));
                admin.createTable(desc);
            }
            if (!admin.isTableEnabled(TableName.valueOf(TABLE_NAME)))
                admin.enableTable(TableName.valueOf(TABLE_NAME));
        }

    }

    @After
    public void truncateTable() throws IOException {
        try (final Admin admin = conn.getAdmin()) {
            admin.disableTable(TableName.valueOf(TABLE_NAME));
            admin.truncateTable(TableName.valueOf(TABLE_NAME), false);
        }
    }

    @Test
    public void insert() throws SiteDaoException, IOException {
        Site site = new Site("http://www.google.com", "welcome to google!");
        Map<String, String> map = new HashMap<>();
        map.put("http://www.stackoverflow.com/google", "see stack");
        map.put("http://www.yahoo.com/google", "see yahoo");
        site.setAnchors(map);
        hbaseSiteDao.insert(site);
        NavigableMap<byte[], byte[]> actualByteMap = hbaseSiteDao.get(site.getReverseLink()).getFamilyMap(Bytes.toBytes(FAMILY_NAME));
        Map<String, String> actual = new HashMap<>();
        for (Map.Entry<byte[], byte[]> entry : actualByteMap.entrySet()) {
            actual.put(Bytes.toString(entry.getKey()), Bytes.toString(entry.getValue()));
        }
        assertEquals(site.getNoProtocolAnchors(), actual);
        final int NUM_OF_TESTS = 10;
        final SecureRandom random = new SecureRandom();
        for (int i = 0; i < NUM_OF_TESTS; i++) {
            site.setLink(site.getLink() + (char) ('a' + random.nextInt(16)));
            hbaseSiteDao.insert(site);
        }
        try (Table table = conn.getTable(TableName.valueOf(TABLE_NAME))) {
            Scan scan = new Scan();
            scan.addFamily(Bytes.toBytes(FAMILY_NAME));
            try (ResultScanner scanner = table.getScanner(scan)) {
                int count = 0;
                for (Result result : scanner) {
                    ++count;
                }
                assertEquals(NUM_OF_TESTS + 1, count);
            }
        }
    }

    @Test
    public void delete() throws SiteDaoException, MalformedURLException {
        Site site = new Site("http://www.google.com", "welcome to google!");
        Map<String, String> map = new HashMap<>();
        map.put("http://www.stackoverflow.com/google", "see stack");
        map.put("http://www.yahoo.com/google", "see yahoo");
        site.setAnchors(map);
        hbaseSiteDao.insert(site);
        hbaseSiteDao.delete(site);
        Result result = hbaseSiteDao.get(site.getReverseLink());
        assertTrue(result.isEmpty());
    }

    @Test
    public void get() throws SiteDaoException, MalformedURLException {
        final int NUM_OF_TESTS = 10;
        Site[] sites = new Site[NUM_OF_TESTS];
        String anchor = "http://www.stackoverflow.com/google";
        String text = "see stack";
        Map<String, String> map = new HashMap<>();
        map.put(anchor, text);
        final SecureRandom random = new SecureRandom();
        for (int i = 0; i < NUM_OF_TESTS; i++) {
            sites[i] = new Site();
            sites[i].setLink("http://www.google.com" + (char)random.nextInt(16));
            sites[i].setAnchors(map);
            hbaseSiteDao.insert(sites[i]);
        }
        for (int i = 0; i < NUM_OF_TESTS; i++) {
            Result result = hbaseSiteDao.get(sites[i].getReverseLink());
            assertEquals(text, Bytes.toString(result.getValue(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes("stackoverflow.com/google"))));
        }
    }

    @Test
    public void contains() throws SiteDaoException {
        Site site = new Site("http://www.google.com", "welcome to google!");
        Map<String, String> map = new HashMap<>();
        map.put("http://www.stackoverflow.com/google", "see stack");
        map.put("http://www.yahoo.com/google", "see yahoo");
        site.setAnchors(map);
        hbaseSiteDao.insert(site);
        assertTrue(hbaseSiteDao.contains(site));
        site = new Site("http://www.yahoo.com", "welcome to yahoo!");
        assertFalse(hbaseSiteDao.contains(site));
    }
}

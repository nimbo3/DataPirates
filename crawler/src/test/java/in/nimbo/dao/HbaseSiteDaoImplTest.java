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
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;

import static org.junit.Assert.*;


public class HbaseSiteDaoImplTest {
    private static final Logger logger = LoggerFactory.getLogger(HbaseSiteDaoImplTest.class);
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
        Site site = new Site("www.google.com", "welcome to google!");
        Map<String, String> map = new HashMap<>();
        map.put("www.stackoverflow.com/google", "see stack");
        map.put("www.yahoo.com/google", "see yahoo");
        site.setAnchors(map);
        site.setReverseLink("com.google.www");
        hbaseSiteDao.insert(site);
        NavigableMap<byte[], byte[]> actualByteMap = hbaseSiteDao.get(site.getReverseLink()).getFamilyMap(Bytes.toBytes("links"));
        Map<String, String> actual = new HashMap<>();
        for (Map.Entry<byte[], byte[]> entry : actualByteMap.entrySet()) {
            actual.put(Bytes.toString(entry.getKey()), Bytes.toString(entry.getValue()));
        }
        assertEquals(site.getAnchors(), actual);
        final int NUM_OF_TESTS = 10;
        final SecureRandom random = new SecureRandom();
        for (int i = 0; i < NUM_OF_TESTS; i++) {
            site.setReverseLink(site.getReverseLink() + (char) ('a' + random.nextInt(16)));
            hbaseSiteDao.insert(site);
        }
        try (Table table = conn.getTable(TableName.valueOf(TABLE_NAME));){
            Scan scan = new Scan();
            scan.addFamily(Bytes.toBytes("links"));
            try(ResultScanner scanner = table.getScanner(scan)){
                int count = 0;
                for (Result result : scanner) {
                    ++count;
                }
                assertEquals(NUM_OF_TESTS + 1, count);
            }
        }
    }

    @Test
    public void delete() throws SiteDaoException {
        Site site = new Site("www.google.com", "welcome to google!");
        Map<String, String> map = new HashMap<>();
        map.put("www.stackoverflow.com/google", "see stack");
        map.put("www.yahoo.com/google", "see yahoo");
        site.setAnchors(map);
        site.setReverseLink("com.google.www");
        hbaseSiteDao.insert(site);
        hbaseSiteDao.delete(site.getReverseLink());
        Result result = hbaseSiteDao.get(site.getReverseLink());
        assertTrue(result.isEmpty());
    }

    @Test
    public void get() throws SiteDaoException {
        Site site = new Site("www.google.com", "welcome to google!");
        Map<String, String> map = new HashMap<>();
        map.put("www.stackoverflow.com/google", "see stack");
        site.setAnchors(map);
        final int NUM_OF_TESTS = 10;
        String[] reverseLinks = new String[NUM_OF_TESTS];
        String[] anchors = new String[NUM_OF_TESTS];
        final SecureRandom random = new SecureRandom();
        site.setReverseLink("org");
        for (int i = 0; i < NUM_OF_TESTS; i++) {
            reverseLinks[i] = site.getReverseLink() + (char) ('a' + random.nextInt(16));
            site.setReverseLink(reverseLinks[i]);
            anchors[i] = "see stack" + (char) ('a' + random.nextInt(16));
            map = new HashMap<>();
            map.put("www.stackoverflow.com/google", anchors[i]);
            site.setAnchors(map);
            hbaseSiteDao.insert(site);
        }
        for (int i = 0; i < NUM_OF_TESTS; i++) {
            Result result = hbaseSiteDao.get(reverseLinks[i]);
            assertEquals(anchors[i], Bytes.toString(result.getValue(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes("www.stackoverflow.com/google"))));
        }
    }

    @Test
    public void contains() throws SiteDaoException {
        Site site = new Site("www.google.com", "welcome to google!");
        Map<String, String> map = new HashMap<>();
        map.put("www.stackoverflow.com/google", "see stack");
        map.put("www.yahoo.com/google", "see yahoo");
        site.setAnchors(map);
        site.setReverseLink("com.google.www");
        hbaseSiteDao.insert(site);
        assertTrue(hbaseSiteDao.contains(site));
        site.setReverseLink("com.helloworld.www");
        assertFalse(hbaseSiteDao.contains(site));
    }
}

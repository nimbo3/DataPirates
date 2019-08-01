package in.nimbo.dao;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import in.nimbo.exception.HbaseSiteDaoException;
import in.nimbo.exception.SiteDaoException;
import in.nimbo.model.Site;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;

import static org.junit.Assert.assertEquals;


public class HbaseSiteDaoImplTest {
    private static HbaseSiteDaoImpl hbaseSiteDao;

    @BeforeClass
    public static void init() throws HbaseSiteDaoException {
        Configuration hBaseConfiguration = HBaseConfiguration.create();
        Config config = ConfigFactory.load("config");
        hbaseSiteDao = new HbaseSiteDaoImpl(hBaseConfiguration, config);
    }

    @Test
    public void insert() throws SiteDaoException {
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
            actual.put(new String(entry.getKey()), new String(entry.getValue()));
        }
        assertEquals(site.getAnchors(), actual);
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

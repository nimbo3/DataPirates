package in.nimbo;

import com.codahale.metrics.SharedMetricRegistries;
import com.typesafe.config.ConfigFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class ElasticDaoTest {
    private static ElasticDao elasticDao;

    @BeforeClass
    public static void init() {
        SharedMetricRegistries.setDefault("data-pirates-keywords");
        elasticDao = new ElasticDao(ConfigFactory.load("config"));
    }

    @Test
    public void updateKeywordsWithIdTest() throws IOException {
//        String[] ids = {"f4fb1ae2a47c5985227d8c24a3e6ae0a803f8376e455e10c9b536e966d1c3d92",
//                "1831ee94cee6d6153bc939f845a586f17e27184e9d00c2751c28acd1f9af6ee0",
//                "9fced93edd3dbf0a168b30847665ae55f49df5651bf7319a19a94491dcd3e347",
//                "2dad0fdbacf9d8f382cf7a840d8e8e14b116f53c11b32e22311ab9327b7f417e"};
        String[] ids = {"1", "2", "3"};
        elasticDao.updateKeywordsWithId(ids);
    }

    @AfterClass
    public static void finish() throws IOException {
        elasticDao.close();
    }
}

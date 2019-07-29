package in.nimbo.database;

import com.codahale.metrics.SharedMetricRegistries;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import in.nimbo.dao.ElasticSiteDaoImpl;
import in.nimbo.exception.SiteDaoException;
import in.nimbo.model.Site;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ElasticSiteDaoImplTest {

    private static ElasticSiteDaoImpl elasticDao;
    private static Config config = ConfigFactory.load("config");

    @BeforeClass
    public static void prepareElasticClient() {
        try {
            SharedMetricRegistries.getDefault();
        } catch (IllegalStateException e) {
            SharedMetricRegistries.setDefault(config.getString("metric.registry.name"));
        }
        Config config = ConfigFactory.load("config");
        elasticDao = new ElasticSiteDaoImpl(config);
    }

    @Test
    public void insertTest() throws SiteDaoException {
        final String LINK = "http://test.test";
        Site site = new Site(LINK, "Test Title");
        elasticDao.insert(site);
        assertEquals(elasticDao.get(LINK), site);
    }
}

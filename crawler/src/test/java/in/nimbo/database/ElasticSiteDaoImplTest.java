package in.nimbo.database;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import in.nimbo.dao.ElasticSiteDaoImpl;
import in.nimbo.exception.SiteDaoException;
import in.nimbo.model.Site;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class ElasticSiteDaoImplTest {

    private static ElasticSiteDaoImpl elasticDao;
    @BeforeClass
    public static void prepareElasticClient() {
        Config config = ConfigFactory.load("config");
        elasticDao = new ElasticSiteDaoImpl(config);
    }

    @Test
    public void insertTest() {
        final String LINK = "http://test.test";
        Site site = new Site(LINK, "Test Title");
        try {
            elasticDao.insert(site);
        } catch (SiteDaoException e) {
            e.printStackTrace();
        }
        assertTrue(elasticDao.get(LINK).equals(site));
    }
}

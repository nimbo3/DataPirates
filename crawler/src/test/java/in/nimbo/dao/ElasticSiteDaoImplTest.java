package in.nimbo.dao;

import com.codahale.metrics.SharedMetricRegistries;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import in.nimbo.exception.ElasticSiteDaoException;
import in.nimbo.exception.SiteDaoException;
import in.nimbo.model.Site;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

import static junit.framework.TestCase.assertTrue;

public class ElasticSiteDaoImplTest {
    private static ElasticSiteDaoImpl elasticDao;
    @BeforeClass
    public static void prepareElasticClient() {
        Config config = ConfigFactory.load("config");
        try {
            SharedMetricRegistries.getDefault();
        } catch (IllegalStateException e) {
            SharedMetricRegistries.setDefault(config.getString("metric.registry.name"));
        }

        elasticDao = new ElasticSiteDaoImpl(config);
        elasticDao.createIndex();
    }

    @Test
    public void insertTest() throws ElasticSiteDaoException {
        final String LINK = "http://test.test";
        Site site = new Site(LINK, "Test Title");
        site.setKeywords("keywords");
        site.setPlainText("text");
        elasticDao.insert(site);
        elasticDao.insert(site);
        System.out.println("inserted");
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        try {
            CountResponse count = elasticDao.client.count(new CountRequest("sites"), RequestOptions.DEFAULT);
            System.out.println(count);
        } catch (IOException e) {
            e.printStackTrace();
        }
        assertTrue(elasticDao.get(LINK).equals(site));
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void insertLinkSizeUpperThan512BytesTest() throws ElasticSiteDaoException {
        expectedException.expect(ElasticSiteDaoException.class);
        expectedException.expectMessage("Elastic Long Id Exception (bytes of id must be lower than 512 bytes)");
        final String LINK = "http://aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                "aaa.test";
        Site site = new Site(LINK, "Test Title");
        site.setKeywords("keywords");
        site.setPlainText("text");
        elasticDao.insert(site);
    }
}
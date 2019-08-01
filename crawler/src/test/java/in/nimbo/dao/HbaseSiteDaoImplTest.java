package in.nimbo.dao;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import in.nimbo.exception.HbaseSiteDaoException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.Test;


public class HbaseSiteDaoImplTest {

    @Test
    public void insert() throws HbaseSiteDaoException {
        Configuration hBaseConfiguration = HBaseConfiguration.create();
        Config config = ConfigFactory.load("config");
        HbaseSiteDaoImpl hbaseSiteDao = new HbaseSiteDaoImpl(hBaseConfiguration, config);
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

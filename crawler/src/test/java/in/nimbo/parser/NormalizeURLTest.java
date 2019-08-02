package in.nimbo.parser;

import com.codahale.metrics.SharedMetricRegistries;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.MalformedURLException;

public class NormalizeURLTest {

    private static final Config config = ConfigFactory.load("config");

    @BeforeClass
    public static void init() {
        try {
            SharedMetricRegistries.getDefault();
        } catch (IllegalStateException e) {
            SharedMetricRegistries.setDefault(config.getString("metric.registry.name"));
        }
    }

    @Test
    public void NormalizerTest() throws MalformedURLException {
        String link = "http://www.google.com/";
        String expected = "http://www.google.com";
        Assert.assertEquals(expected, NormalizeURL.normalize(link));
        link = "https://www.google.com/?b=y&a=x";
        expected = "https://www.google.com?a=x&b=y";
        Assert.assertEquals(expected, NormalizeURL.normalize(link));
    }

}
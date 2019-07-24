package in.nimbo;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class FetcherImplTest {

    private static Config config;

    @BeforeClass
    public static void init() {
        config = ConfigFactory.load("config.properties");
    }

    @Test
    public void fetch() throws IOException {
        FetcherImpl fetcher = new FetcherImpl(config);
        fetcher.fetch("http://httpbin.org/redirect/3");
        Assert.assertEquals("http://httpbin.org/get", fetcher.getRedirectUrl());
        fetcher.fetch("http://bit.ly/2Y0QwLF");
        Assert.assertEquals("https://git-scm.com/docs/git-credential-store", fetcher.getRedirectUrl());

    }
}
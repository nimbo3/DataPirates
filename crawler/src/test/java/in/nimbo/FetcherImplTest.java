package in.nimbo;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.http.client.RedirectException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class FetcherImplTest {

    private static Config config;

    @BeforeClass
    public static void init() {
        config = ConfigFactory.load("config.properties");
    }

    @Test
    public void fetcherRedirectionTest() throws IOException {
        FetcherImpl fetcher = new FetcherImpl(config);
        int maxRedirects = config.getInt("fetcher.max.redirects");
        fetcher.fetch(String.format("http://httpbin.org/redirect/%d", maxRedirects));
        Assert.assertEquals("http://httpbin.org/get", fetcher.getRedirectUrl());
        fetcher.fetch("http://bit.ly/2Y0QwLF");
        Assert.assertEquals("https://git-scm.com/docs/git-credential-store", fetcher.getRedirectUrl());

    }

    @Test(expected = IOException.class)
    public void fetcherMaxRedirectionTest() throws IOException {
        FetcherImpl fetcher = new FetcherImpl(config);
        int maxRedirects = config.getInt("fetcher.max.redirects");
        fetcher.fetch(String.format("http://httpbin.org/redirect/%d", maxRedirects+1));
    }

}
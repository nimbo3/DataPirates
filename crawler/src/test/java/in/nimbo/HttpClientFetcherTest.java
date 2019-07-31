package in.nimbo;

import com.codahale.metrics.SharedMetricRegistries;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import in.nimbo.exception.FetchException;
import in.nimbo.fetch.HttpClientFetcher;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class HttpClientFetcherTest {
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
    public void fetcherRedirectionTest() throws FetchException {
        HttpClientFetcher fetcher = new HttpClientFetcher(config);
        int maxRedirects = config.getInt("fetcher.max.redirects");
        fetcher.fetch(String.format("http://httpbin.org/redirect/%d", maxRedirects));
        Assert.assertEquals("http://httpbin.org/get", fetcher.getRedirectUrl());
        fetcher.fetch("http://bit.ly/2Y0QwLF");
        Assert.assertEquals("https://git-scm.com/docs/git-credential-store", fetcher.getRedirectUrl());
    }

    @Test(expected = FetchException.class)
    public void fetcherMaxRedirectionTest() throws FetchException {
        HttpClientFetcher fetcher = new HttpClientFetcher(config);
        int maxRedirects = config.getInt("fetcher.max.redirects");
        fetcher.fetch(String.format("http://httpbin.org/redirect/%d", maxRedirects + 1));
    }

}
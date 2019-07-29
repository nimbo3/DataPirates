package in.nimbo;

import com.codahale.metrics.SharedMetricRegistries;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import in.nimbo.exception.FetchException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class FetcherImplTest {
    private static Config config;

    @BeforeClass
    public static void init() {
        SharedMetricRegistries.setDefault("data-pirates-crawler");
        config = ConfigFactory.load("config.properties");
    }

    @Test
    public void fetcherRedirectionTest() throws IOException {
        FetcherImpl fetcher = new FetcherImpl(config);
        int maxRedirects = config.getInt("fetcher.max.redirects");
        try {
            fetcher.fetch(String.format("http://httpbin.org/redirect/%d", maxRedirects));
        } catch (FetchException e) {
            e.printStackTrace();
        }
        Assert.assertEquals("http://httpbin.org/get", fetcher.getRedirectUrl());
        try {
            fetcher.fetch("http://bit.ly/2Y0QwLF");
        } catch (FetchException e) {
            e.printStackTrace();
        }
        Assert.assertEquals("https://git-scm.com/docs/git-credential-store", fetcher.getRedirectUrl());

    }

    @Test(expected = FetchException.class)
    public void fetcherMaxRedirectionTest() throws FetchException {
        FetcherImpl fetcher = new FetcherImpl(config);
        int maxRedirects = config.getInt("fetcher.max.redirects");
        fetcher.fetch(String.format("http://httpbin.org/redirect/%d", maxRedirects + 1));
    }

}
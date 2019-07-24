package in.nimbo;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class FetcherImplTest {

    @Test
    public void fetch() throws IOException {
        FetcherImpl fetcher = new FetcherImpl();
        fetcher.fetch("http://httpbin.org/redirect/3");
        Assert.assertEquals("http://httpbin.org/get", fetcher.getRedirectUrl());
        fetcher.fetch("http://bit.ly/2Y0QwLF");
        Assert.assertEquals("https://git-scm.com/docs/git-credential-store", fetcher.getRedirectUrl());

    }
}
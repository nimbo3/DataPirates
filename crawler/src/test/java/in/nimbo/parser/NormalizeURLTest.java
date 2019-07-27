package in.nimbo.parser;

import org.junit.Assert;
import org.junit.Test;

import java.net.MalformedURLException;

public class NormalizeURLTest {

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
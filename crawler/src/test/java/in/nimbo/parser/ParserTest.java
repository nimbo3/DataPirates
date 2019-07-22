package in.nimbo.parser;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class ParserTest {
    private static Config config;
    private static String link = "";

    @BeforeClass
    public static void init() throws FileNotFoundException {
        config = ConfigFactory.load("config");
    }

    @Test
    public void extractMetadataTest() throws IOException {
        String html;
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream("html/parserTest2.html")) {
            assert inputStream != null;
            html = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        }
        Parser parser = new Parser(link, html);
        System.out.println(parser.extractMetadata());
    }

    @Test
    public void extractTitleTest() throws IOException {
        String html;
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream("html/parserTest2.html")) {
            assert inputStream != null;
            html = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        }
        Parser parser = new Parser(link, html);
        System.out.println(parser.extractTitle());
    }

    @Test
    public void extractPlainTextTest() throws IOException {
        String html;
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream("html/parserTest2.html")) {
            assert inputStream != null;
            html = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        }
        Parser parser = new Parser(link, html);
        System.out.println(parser.extractPlainText());
//        Assert.assertTrue();
    }

    // this part is wrong shit :o ++:D
    @Test
    public void extractKeywordsTest() throws IOException {
        String html;
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream("html/parserTest2.html")) {
            assert inputStream != null;
            html = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        }
        Parser parser = new Parser(link, html);
        System.out.println(parser.extractKeywords());
    }

}
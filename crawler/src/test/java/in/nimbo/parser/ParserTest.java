package in.nimbo.parser;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.io.IOUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class ParserTest {
    private static Config config;

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
        String link = config.getString("parser.link");
        Parser parser = new Parser(link, html);
        parser.extractMetadata();
    }

}
package in.nimbo.parser;

import com.codahale.metrics.SharedMetricRegistries;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import in.nimbo.model.Site;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class ParserTest {
    private static final Config config = ConfigFactory.load("config");
    private static final double CONFIDENCE = config.getDouble("Parser.confidence");
    private static final int NUM_OF_TESTS = config.getInt("Parser.numOfTests");
    private static String[] htmls = new String[NUM_OF_TESTS];
    private static Site[] sites = new Site[NUM_OF_TESTS];
    private String[] links = {config.getString("Parser.link1"), config.getString("Parser.link2")};

    @BeforeClass
    public static void init() throws IOException {
        try {
            SharedMetricRegistries.getDefault();
        } catch (IllegalStateException e) {
            SharedMetricRegistries.setDefault(config.getString("metric.registry.name"));
        }
        for (int i = 0; i < NUM_OF_TESTS; i++) {
            try (InputStream inputStream =
                         ParserTest.class.getClassLoader().getResourceAsStream(
                                 "html/" + "parserTest" + (i + 1) + ".html")) {
                assert inputStream != null;
                htmls[i] = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
            }
            try (InputStream inputStream =
                         ParserTest.class.getClassLoader().getResourceAsStream("parserTest" + (i + 1) + ".txt")) {
                StringBuilder sb = new StringBuilder();
                assert inputStream != null;
                Scanner scanner = new Scanner(inputStream);
                scanner.nextLine();//ignore "painText:" string
                String string;
                sites[i] = new Site();
                while (scanner.hasNextLine()) {
                    string = scanner.nextLine();
                    if (string.contains("keywords:")) {
                        sites[i].setPlainText(sb.toString());
                        sb = new StringBuilder();
                    } else if (string.contains("title:")) {
                        sites[i].setKeywords(sb.toString());
                        sb = new StringBuilder();
                    } else if (string.contains("metadata:")) {
                        sites[i].setTitle(sb.toString());
                        sb = new StringBuilder();
                    } else if (string.contains("anchors:")) {
                        sites[i].setMetadata(sb.toString());
                        break;
                    } else {
                        sb.append(string).append(" ");
                    }
                }
                HashMap<String, String> anchors = new HashMap<>();
                while (scanner.hasNextLine()) {
                    String[] parts = scanner.nextLine().split("=");
                    anchors.put(parts[0], parts.length > 1 ? parts[1] : "");
                }
                sites[i].setAnchors(anchors);
            }
        }
    }

    private double getPercentage(String expected, String actual) {
        String[] actualWords = actual.split("[!.?:,;\\s]");
        Set<String> words = new HashSet<>();
        Collections.addAll(words, expected.split("[!.?:,;\\s]"));
        String[] uniqueWords = words.toArray(new String[0]);
        int uniqueWordsCounter = 0;
        for (String s : actualWords) {
            for (String uniqueWord : uniqueWords) {
                if (uniqueWord.contains(s) || s.contains(uniqueWord)) {
                    ++uniqueWordsCounter;
                    break;
                }
            }
        }
        return ((double) uniqueWordsCounter / (double) actualWords.length) * 100;
    }

    @Test
    public void extractMetadataTest() {
        for (int i = 0; i < NUM_OF_TESTS; i++) {
            Parser parser = new Parser(links[i], htmls[i]);
            String actual = parser.extractMetadata();
            String expected = sites[i].getMetadata();
            double percentage = getPercentage(expected, actual);
        }
    }

    @Test
    public void extractTitleTest() {
        for (int i = 0; i < NUM_OF_TESTS; i++) {
            Parser parser = new Parser(links[i], htmls[i]);
            String actual = parser.extractTitle();
            String expected = sites[i].getTitle();
            double percentage = getPercentage(expected, actual);
            Assert.assertTrue(percentage >= CONFIDENCE);
        }
    }

    @Test
    public void extractPlainTextTest() {
        for (int i = 0; i < NUM_OF_TESTS; i++) {
            Parser parser = new Parser(links[i], htmls[i]);
            String actual = parser.extractPlainText();
            String expected = sites[i].getPlainText();
            double percentage = getPercentage(expected, actual);
            Assert.assertTrue(percentage >= CONFIDENCE);
        }
    }

    @Test
    public void extractKeywordsTest() {
        for (int i = 0; i < NUM_OF_TESTS; i++) {
            Parser parser = new Parser(links[i], htmls[i]);
            String actual = parser.extractKeywords();
            String expected = sites[i].getKeywords();
            double percentage = getPercentage(expected, actual);
            Assert.assertTrue(percentage >= CONFIDENCE);
        }
    }

    @Test
    public void extractAnchorsTest() {
        for (int i = 0; i < NUM_OF_TESTS; i++) {
            Parser parser = new Parser(links[i], htmls[i]);
            Map<String, String> actualList = parser.extractAnchors();
            Map<String, String> expectedList = sites[i].getAnchors();
            StringBuilder actual = new StringBuilder(), expected = new StringBuilder();
            for (String s : actualList.keySet()) {
                actual.append(s).append(" ");
            }
            for (String s : expectedList.keySet()) {
                expected.append(s).append(" ");
            }
            double percentage = getPercentage(expected.toString(), actual.toString());
            Assert.assertTrue(percentage >= CONFIDENCE);


            actual = new StringBuilder();
            expected = new StringBuilder();
            for (String s : actualList.values()) {
                actual.append(s).append(" ");
            }
            for (String s : expectedList.values()) {
                expected.append(s).append(" ");
            }
            percentage = getPercentage(expected.toString(), actual.toString());
            Assert.assertTrue(percentage >= CONFIDENCE);
        }
    }


    @Test
    public void reverseTest() throws IOException {
        Parser parser = new Parser(links[0], htmls[0]);
        String url = "https://www.geeksforgeeks.org:80/url-samefile-method-in-java-with-examples/";
        String expected = "org.geeksforgeeks:80/url-samefile-method-in-java-with-examples/";
        Assert.assertEquals(expected, parser.reverse(url));
        expected = "org.apache.spark/documentation.html";
        url = "http://www.spark.apache.org/documentation.html";
        Assert.assertEquals(expected, parser.reverse(url));
        expected = "com.stackoverflow/questions/7569335/reverse-a-string-in-java";
        url = "https://stackoverflow.com/questions/7569335/reverse-a-string-in-java";
        Assert.assertEquals(expected, parser.reverse(url));
        expected = "master:16010/table.jsp?name=wc";
        url = "http://master:16010/table.jsp?name=wc";
        Assert.assertEquals(expected, parser.reverse(url));
        expected = "ir.ac.sbu.znu.samp";
        url = "https://samp.znu.sbu.ac.ir";
        Assert.assertEquals(expected, parser.reverse(url));
        url = "https://samp.znu.sbu.ac.ir/www.asd";
    }
    @Test
    public void normalizeTest() throws MalformedURLException {
        Parser parser = new Parser(links[0], htmls[0]);
        String url = "https://www.geeksforgeeks.org:80/url-samefile-method-in-java-with-examples/";
        String expected = "http://geeksforgeeks.org/url-samefile-method-in-java-with-examples";
        Assert.assertEquals(expected, parser.normalize(url));
        url = "http://www.googlewww.com";
        expected = "http://googlewww.com";
        Assert.assertEquals(expected, parser.normalize(url));
        url = "http://www.yahoo.com/asdwaefselkjhklsd#slfdjslkdj";
        expected = "http://yahoo.com/asdwaefselkjhklsd";
        Assert.assertEquals(expected, parser.normalize(url));
        url = "http://www.yahoo.com/werljwer?";
        expected = "http://yahoo.com/werljwer";
        Assert.assertEquals(expected, parser.normalize(url));
        url = "http://www.yahoo.com/asdwaefselkjhklsd?asghar=2";
        expected = "http://yahoo.com/asdwaefselkjhklsd?asghar=2";
        Assert.assertEquals(expected, parser.normalize(url));
    }
}
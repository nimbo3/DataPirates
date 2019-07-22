package in.nimbo.parser;

import in.nimbo.model.Site;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class ParserTest {
    private static final double confidence = 0.80;
    private static final int numOfTests = 2;
    private static String[] htmls = new String[numOfTests];
    private static Site[] sites = new Site[numOfTests];
    String[] links = {"http://www.jsoup.org", "http://www.york.ac.uk/teaching/cws/wws"};

    @BeforeClass
    public static void init() throws IOException {
        for (int i = 0; i < numOfTests; i++) {
            try (InputStream inputStream =
                         ParserTest.class.getClassLoader().getResourceAsStream("html/parserTest" + (i + 1) + ".html")) {
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
                List<String> anchors = new LinkedList<>();
                while (scanner.hasNextLine()) {
                    anchors.add(scanner.nextLine());
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
        for (int i = 0; i < numOfTests; i++) {
            Parser parser = new Parser(links[i], htmls[i]);
            String actual = parser.extractMetadata();
            String expected = sites[i].getMetadata();
            double percentage = getPercentage(expected, actual);
            System.out.println(percentage);
        }
    }

    @Test
    public void extractTitleTest() {
        for (int i = 0; i < numOfTests; i++) {
            Parser parser = new Parser(links[i], htmls[i]);
            String actual = parser.extractTitle();
            String expected = sites[i].getTitle();
            double percentage = getPercentage(expected, actual);
            Assert.assertTrue(percentage >= confidence);
        }
    }

    @Test
    public void extractPlainTextTest() {
        for (int i = 0; i < numOfTests; i++) {
            Parser parser = new Parser(links[i], htmls[i]);
            String actual = parser.extractPlainText();
            String expected = sites[i].getPlainText();
            double percentage = getPercentage(expected, actual);
            Assert.assertTrue(percentage >= confidence);
        }
    }

    @Test
    public void extractKeywordsTest() {
        for (int i = 0; i < numOfTests; i++) {
            Parser parser = new Parser(links[i], htmls[i]);
            String actual = parser.extractKeywords();
            String expected = sites[i].getKeywords();
            double percentage = getPercentage(expected, actual);
            Assert.assertTrue(percentage >= confidence);
        }
    }

    @Test
    public void extractAnchorsTest() {
        for (int i = 0; i < numOfTests; i++) {
            Parser parser = new Parser(links[i], htmls[i]);
            List<String> actualList = parser.extractAnchors();
            List<String> expectedList = sites[i].getAnchors();
            StringBuilder actual = new StringBuilder(), expected = new StringBuilder();
            for (String s : actualList) {
                actual.append(s).append(" ");
            }
            for (String s : expectedList) {
                expected.append(s).append(" ");
            }
            double percentage = getPercentage(expected.toString(), actual.toString());
            Assert.assertTrue(percentage >= confidence);
        }
    }
}
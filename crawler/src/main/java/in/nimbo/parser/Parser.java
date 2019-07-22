package in.nimbo.parser;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.util.LinkedList;
import java.util.List;

public class Parser {
    private static final Logger logger = LoggerFactory.getLogger(Parser.class);
    private String link;
    private Document document;

    public Parser(String link, String html) {
        this.link = link;
        document = Jsoup.parse(html, link);
    }

    /**
     * finds the <title>title</title> part in the header of the html which is shown on each tab opened by some browsers
     *
     * @return string containing the title of html
     */
    public String extractTitle() {
        return document.title();
    }

    public String extractPlainText() {
        return document.text();
    }

    /**
     * finds contents that are bold or headers, and contents of all meta tags having "name=description" or "name=keyword"
     *
     * @return a space-seperated-string that appends all values mentioned
     */
    public String extractKeywords() {
        Elements elements = document.select("h1 > *, h2 > *, h3 > *, h4 > *, h5 > *,b");
        StringBuilder sb = new StringBuilder();
        sb.append(elements.text()).append(" ");
        Elements metaTags = document.getElementsByTag("meta");
        for (Element metaTag : metaTags) {
            String content = metaTag.attr("content");
            String name = metaTag.attr("name");
            if (name.contains("description"))
                sb.append(content).append(" ");
            else if (name.contains("keyword"))
                sb.append(content).append(" ");
        }
        return sb.toString();
    }

    public List<String> extractAnchors() {
        Elements elements = document.select("a");
        List<String> list = new LinkedList<>();
        String href = "";
        for (Element element : elements) {
            try {
                href = element.absUrl("href");
                list.add(NormalizeURL.normalize(href));
            } catch (MalformedURLException e) {
                logger.debug("normalizer can't add link: " + href + " to the anchors list for this page: " + link, e);
            }
        }
        return list;
    }

    public String extractMetadata() {
        Elements metaTags = document.getElementsByTag("meta");
        StringBuilder sb = new StringBuilder();
        for (Element metaTag : metaTags) {
            sb.append(metaTag.attributes());
        }
        return sb.toString();
    }
}

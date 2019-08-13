package in.nimbo.parser;

import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import in.nimbo.model.Site;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class Parser {
    private static final Logger logger = LoggerFactory.getLogger(Parser.class);
    private static Timer parseTimer = SharedMetricRegistries.getDefault().timer("parser");
    private String link;
    private Document document;
    private String html;

    public Parser(String link, String html) throws MalformedURLException {
        this.html = html;
        if (!link.matches("^\\w+://"))
            link = link + "http://";
        if (hasBadProtocol(link)) {
            throw new MalformedURLException("protocol is not supported for:" + link + ". Only http/https are supported");
        }
        link = normalize(link);
        this.link = link;
        document = Jsoup.parse(html, link);
    }

    public static String getDomain(String link) throws MalformedURLException {
        URL url = new URL(link);
        return url.getHost();
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
        Elements elements = document.select("h1 > *, h2 > *, h3 > *,b");
        StringBuilder sb = new StringBuilder();
        sb.append(elements.text()).append(" ");
        Elements metaTags = document.getElementsByTag("meta");
        for (Element metaTag : metaTags) {
            String content = metaTag.attr("content");
            String name = metaTag.attr("name");
            if (name.contains("description") || name.contains("keyword"))
                sb.append(content).append(" ");
        }
        return sb.toString();
    }

    public Map<String, String> extractAnchors() {
        Elements elements = document.select("a[href]");
        Map<String, String> map = new HashMap<>();
        String href = "";
        for (Element element : elements) {
            href = element.absUrl("abs:href");
            String content = element.text();
            if (content.length() == 0)
                content = "empty";
            try {
                if (!href.matches("^\\w+://"))
                    href = href + "http://";
                if (hasBadProtocol(href)) {
                    logger.error("protocol is not supported for:" + href + ". Only http/https are supported");
                    continue;
                }
                href = normalize(href);
                if (map.containsKey(href))
                    map.put(href, map.get(href) + ", " + content);
                else
                    map.put(href, content);
            } catch (MalformedURLException e) {
                logger.error("url is not well-formed for:" + href + ". can't normalize this url.");
            }
        }
        if (map.size() == 0)
            map.put(link, "empty");
        return map;
    }

    public String normalize(String href) throws MalformedURLException {
        href = NormalizeURL.normalize(href);
        URL url = new URL(href);
        String domain = url.getHost().replaceFirst("www\\.", "");
        return href.replace(url.getHost(), domain);
    }

    public String extractMetadata() {
        Elements metaTags = document.getElementsByTag("meta");
        StringBuilder sb = new StringBuilder();
        for (Element metaTag : metaTags) {
            sb.append(metaTag.attributes());
        }
        return sb.toString();
    }

    public Site parse() {
        try (Timer.Context time = parseTimer.time()) {
            Site site = new Site();
            site.setTitle(extractTitle());
            site.setKeywords(extractKeywords());
            site.setMetadata(extractMetadata());
            site.setAnchors(extractAnchors());
            site.setPlainText(extractPlainText());
            site.setLink(link);
            site.setHtml(html);
            return site;
        }
    }

    private boolean hasBadProtocol(String urlString) throws MalformedURLException {
        URL url = new URL(urlString);
        return !url.getProtocol().equals("http") && !url.getProtocol().equals("https");
    }
}

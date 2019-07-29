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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Parser {
    private static final Logger logger = LoggerFactory.getLogger(Parser.class);
    private static Timer parseTimer = SharedMetricRegistries.getDefault().timer("parser");
    private String link;
    private Document document;
    private String html;

    public Parser(String link, String html) {
        this.html = html;
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
        Elements elements = document.select("h1 > *, h2 > *, h3 > *, h4 > *, h5 > *,b");
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
            try {
                href = element.absUrl("abs:href");
                String content = element.text();
                if (content.length() == 0)
                    content = "empty";
                href = NormalizeURL.normalize(href);
                if (validateProtocol(href))
                    map.put(href, content);
                else
                    logger.debug("protocol is not supported for:" + href + ". Only http/https are supported");
            } catch (MalformedURLException e) {
                logger.debug("normalizer can't add link: " + href + " to the anchors list for this page: " + link, e);
            }
        }
        if (map.size() == 0)
            map.put(href, "empty");
        return map;
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
            site.setReverseLink(reverse(link));
            site.setHtml(html);
            return site;
        } catch (MalformedURLException e) {
            logger.error("can't make reverse link for key in pareser.", e);
        }
        return null;
    }

    public String reverse(String link) throws MalformedURLException {
        URL url = new URL(link);
        String domain = url.getHost();
        domain = domain.replaceAll(" ", "");
        StringBuilder sb = new StringBuilder(domain).reverse();
        StringBuilder reverse = new StringBuilder();
        int index = -1;
        do {
            int startIndex = index + 1;
            index = sb.indexOf(".", index + 1);
            if (index != -1) {
                reverse.append(new StringBuilder(sb.substring(startIndex, index)).reverse());
                reverse.append(".");
            } else {
                reverse.append(new StringBuilder(sb.substring(startIndex)).reverse());
            }
        } while (index != -1);
        return link.replace(domain, reverse).replaceAll("https?://", "").replace(".www", "");
    }

    private boolean validateProtocol(String urlString) throws MalformedURLException {
        URL url = new URL(urlString);
        return url.getProtocol().equals("http") || url.getProtocol().equals("https");
    }
}

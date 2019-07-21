package in.nimbo.parser;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.util.List;

public class Parser {
    private String link;
    private String html;
    private Document document;

    public Parser(String link, String html){
        this.link = link;
        this.html = html;
        document = Jsoup.parse(html);
    }

    public String extractTitle(){
        return document.title();
    }

    public String extractLink(){
        return link;
    }

    public String extractPlainText(){
        return document.text();
    }

    public String extractKeywords(){
        Elements elements = document.select("h1 > *, h2 > *, h3 > *, h4 > *, h5 > *,b");
        return elements.text();
    }

    public List<String> extractAnchors(){
        Elements elements = document.select("a[href]");
        return elements.eachText();
    }

    public String extractMetadata(){
        Elements metaTags = document.getElementsByTag("meta");

        for (Element metaTag : metaTags) {
            System.out.println(metaTag.attributes());
            String content = metaTag.attr("content");
            String name = metaTag.attr("name");
//
//            if("d.title".equals(name)) {
//                ex.setTitle(content);
//            }
//            if("d.description".equals(name)) {
//                ex.setDescription(content);
//            }
//            if("d.language".equals(name)) {
//                ex.setLanguage(content);
//            }
        }
        return null;
    }
}

package in.nimbo.parser;

import java.util.List;

public class Site {
    private String link;
    private String title;
    private String keywords;
    private String plainText;
    private String metadata;
    private List<String> anchors;

    public Site(String link, String title, String keywords, String plainText, List<String> anchors) {
        this.link = link;
        this.title = title;
        this.keywords = keywords;
        this.plainText = plainText;
        this.anchors = anchors;
    }

    public void setLink(String link) {
        this.link = link;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void setKeywords(String keywords) {
        this.keywords = keywords;
    }

    public void setPlainText(String plainText) {
        this.plainText = plainText;
    }

    public void setAnchors(List<String> anchors) {
        this.anchors = anchors;
    }

    public String getLink() {
        return link;
    }

    public String getTitle() {
        return title;
    }

    public String getKeywords() {
        return keywords;
    }

    public String getPlainText() {
        return plainText;
    }

    public List<String> getAnchors() {
        return anchors;
    }

    public String getMetadata() {
        return metadata;
    }

    public void setMetadata(String metadata) {
        this.metadata = metadata;
    }
}

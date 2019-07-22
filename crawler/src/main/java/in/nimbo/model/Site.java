package in.nimbo.model;

import java.util.List;

public class Site {
    private String link;
    private String title;
    private String keywords;
    private String plainText;
    private String metadata;
    private List<String> anchors;

    public Site() {
    }

    public Site(String link, String title) {
        this.link = link;
        this.title = title;
    }


    public String getLink() {
        return link;
    }

    public void setLink(String link) {
        this.link = link;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getKeywords() {
        return keywords;
    }

    public void setKeywords(String keywords) {
        this.keywords = keywords;
    }

    public String getPlainText() {
        return plainText;
    }

    public void setPlainText(String plainText) {
        this.plainText = plainText;
    }

    public List<String> getAnchors() {
        return anchors;
    }


    public void setAnchors(List<String> anchors) {
        this.anchors = anchors;
    }

    public String getMetadata() {
        return metadata;
    }

    public void setMetadata(String metadata) {
        this.metadata = metadata;
    }

    @Override
    public String toString() {
        return plainText + "\n" + keywords + "\n" + title + "\n" + metadata + "\n" + anchors;
    }
}

package in.nimbo.model;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class Site {
    private String link;
    private String title;
    private String keywords;
    private String plainText;
    private String metadata;
    private String html;
    private Map<String, String> anchors;

    public Site() {
    }

    public Site(String link, String title) {
        this.link = link;
        this.title = title;
    }

    public String getReverseLink() throws MalformedURLException {
        URL url = new URL(link);
        final String[] splits = url.getHost().split("\\.");
        StringBuilder reverse = new StringBuilder();
        for (int i = splits.length - 1; i >= 0; i--) {
            reverse.append(splits[i]);
            reverse.append(".");
        }
        if (reverse.charAt(reverse.length() - 1) == '.')
            reverse.deleteCharAt(reverse.length() - 1);
        return link.replace(url.getHost(), reverse).replaceFirst("https?://", "").
                replaceFirst("\\.www", "");
    }

    public String getHtml() {
        return html;
    }

    public void setHtml(String html) {
        this.html = html;
    }

    public String getLink() {
        return link;
    }

    public void setLink(String link) {
        this.link = link;
    }

    public String getNoProtocolLink() {
        return link.replaceFirst("https?://", "");
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

    public Map<String, String> getAnchors() {
        return anchors;
    }

    public void setAnchors(Map<String, String> anchors) {
        this.anchors = anchors;
    }

    public Map<String, String> getNoProtocolAnchors() {
        Map<String, String> map = new HashMap<>();
        for (Map.Entry<String, String> entry : anchors.entrySet()) {
            map.put(entry.getKey().replaceFirst("https?://", "").
                            replaceFirst("www\\.", ""), entry.getValue());
        }
        return map;
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

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Site))
            return false;
        Site that = (Site) o;
        return this.link.equals(that.link);
    }

    @Override
    public int hashCode() {
        int result = 0;
        result = result * 37 + (link != null ? (link.hashCode()) : 0);
        return result;
    }
}

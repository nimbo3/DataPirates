package in.nimbo.model;

import java.util.List;

public class ResultEntry {
    private String title;
    private String link;
    private String summary;
    private double pageRank;
    private List<String> tags;

    public ResultEntry(String title, String link, String summary, double pageRank, List<String> tags) {
        this.title = title;
        this.link = link;
        this.summary = summary;
        this.pageRank = pageRank;
        this.tags = tags;
    }

    public String getTitle() {
        return title;
    }

    public String getLink() {
        return link;
    }

    public String getSummary() {
        return summary;
    }

    public double getPageRank() {
        return pageRank;
    }

    public List<String> getTags() {
        return tags;
    }
}


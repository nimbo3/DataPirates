package in.nimbo.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class ResultEntry {
    @JsonProperty("title")
    private String title;
    @JsonProperty("link")
    private String link;
    @JsonProperty("text")
    private String summary;
    @JsonProperty("rank")
    private double pageRank;
    @JsonProperty("tags")
    private List<String> tags;

    public ResultEntry(String title, String link, String summary) {
        this.title = title;
        this.link = link;
        this.summary = summary;
    }

    public void setPageRank(double pageRank) {
        this.pageRank = pageRank;
    }

    public void setTags(List<String> tags) {
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


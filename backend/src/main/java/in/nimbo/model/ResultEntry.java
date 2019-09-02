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
    @JsonProperty("full-text")
    private String text;
    @JsonProperty("rank")
    private double pageRank;
    @JsonProperty("tags")
    private List<String> tags;
    @JsonProperty("cluster_id")
    private int clusterId;
    @JsonProperty("cluster_label")
    private String[] clusterLabel;


    public ResultEntry(String title, String link, String text, String summary) {
        this.title = title;
        this.link = link;
        this.text = text;
        this.summary = summary;
    }

    public int getClusterId() {
        return clusterId;
    }

    public void setClusterId(int clusterId) {
        this.clusterId = clusterId;
    }

    public String[] getClusterLabel() {
        return clusterLabel;
    }

    public void setClusterLabel(String[] clusterLabel) {
        this.clusterLabel = clusterLabel;
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

    public String getText() {
        return text;
    }

    public double getPageRank() {
        return pageRank;
    }

    public void setPageRank(double pageRank) {
        this.pageRank = pageRank;
    }

    public List<String> getTags() {
        return tags;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }
}


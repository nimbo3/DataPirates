package in.nimbo.model;

import java.io.Serializable;

public class UpdateObject implements Serializable {
    String id;
    String url;
    double pageRank;

    public UpdateObject(String id, String url, double pageRank) {
        this.id = id;
        this.url = url;
        this.pageRank = pageRank;
    }

    public UpdateObject() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public double getPageRank() {
        return pageRank;
    }

    public void setPageRank(double pageRank) {
        this.pageRank = pageRank;
    }
}

package in.nimbo;

import java.io.Serializable;

public class UpdateObject implements Serializable {
    String id;
    double pageRank;

    public UpdateObject(String id, double pageRank) {
        this.id = id;
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

    public double getPageRank() {
        return pageRank;
    }

    public void setPageRank(double pageRank) {
        this.pageRank = pageRank;
    }
}

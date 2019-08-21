package in.nimbo;

import java.io.Serializable;

public class UpdateObject implements Serializable {
    String id;
    double pageRank;

    public UpdateObject(String id, double pageRank) {
        this.id = id;
        this.pageRank = pageRank;
    }
}

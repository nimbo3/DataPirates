package in.nimbo.model;

public class Edge {
    String srdId;
    String dstId;

    public Edge(String srdId, String dstId) {
        this.srdId = srdId;
        this.dstId = dstId;
    }

    public String getSrdId() {
        return srdId;
    }

    public void setSrdId(String srdId) {
        this.srdId = srdId;
    }

    public String getDstId() {
        return dstId;
    }

    public void setDstId(String dstId) {
        this.dstId = dstId;
    }
}

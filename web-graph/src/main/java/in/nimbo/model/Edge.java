package in.nimbo.model;

import java.io.Serializable;

public class Edge implements Serializable {
    String src;
    String dst;
    Integer weight;

    public Edge(String src, String dst) {
        this.src = src;
        this.dst = dst;
        this.weight = 1;
    }

    public String getSrc() {
        return src;
    }

    public void setSrc(String src) {
        this.src = src;
    }

    public String getDst() {
        return dst;
    }

    public void setDst(String dst) {
        this.dst = dst;
    }

    public Integer getWeight() {
        return weight;
    }

    public void setWeight(Integer weight) {
        this.weight = weight;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof Edge &&
                ((Edge) obj).src.equals(this.src) &&
                ((Edge) obj).dst.equals(this.dst);
    }
}
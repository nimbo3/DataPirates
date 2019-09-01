package in.nimbo.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

public class Edge implements Serializable {
    @JsonProperty("from")
    String src;
    @JsonProperty("to")
    String dst;
    @JsonProperty("width")
    int weight;
    String arrows = "to";

    public Edge(String src, String dst) {
        this.src = src;
        this.dst = dst;
        this.weight = 1;
    }

    public String getArrows() {
        return arrows;
    }

    public Edge(String src, String dst, int weight) {
        this.src = src;
        this.dst = dst;
        this.weight = weight;
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

    @Override
    public int hashCode() {
        String uniqueId = src + dst;
        char[] value = uniqueId.toCharArray();
        int h = 0;
        if (value.length > 0) {
            char val[] = value;

            for (int i = 0; i < value.length; i++) {
                h = 31 * h + val[i];
            }
        }
        return h;
    }
}
package in.nimbo.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

public class Vertex implements Serializable {
    @JsonProperty("id")
    String id;
    @JsonProperty("label")
    String label;
    @JsonProperty("color")
    String color;

    public Vertex(String id) {
        this.id = id;
        this.label = id;
        this.color = "#cbf1f7";
    }

    public Vertex(String id, String color) {
        this.id = id;
        this.label = id;
        this.color = color;
    }

    public String getColor() {
        return color;
    }

    public void setColor(String color) {
        this.color = color;
    }

    public String getId() {
        return id;
    }

    public String getLabel() {
        return label;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public boolean equals(Object obj) {
        return obj  instanceof Vertex &&
                ((Vertex) obj).id.equals(this.id);
    }

    @Override
    public int hashCode() {
        String uniqueId = id;
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
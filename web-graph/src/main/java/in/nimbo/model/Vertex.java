package in.nimbo.model;

import java.io.Serializable;

public class Vertex implements Serializable {
    String id;

    public Vertex(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public boolean equals(Object obj) {
        return obj  instanceof Vertex &&
                ((Vertex) obj).id.equals(this.id);
    }
}
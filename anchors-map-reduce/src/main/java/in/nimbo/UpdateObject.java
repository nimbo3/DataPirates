package in.nimbo;

import java.io.Serializable;

public class UpdateObject implements Serializable {
    String id;
    int numOfRefs;

    public UpdateObject(String id, int numOfRefs) {
        this.id = id;
        this.numOfRefs = numOfRefs;
    }
}

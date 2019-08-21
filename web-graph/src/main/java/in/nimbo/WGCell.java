package in.nimbo;

import java.io.Serializable;

public class WGCell implements Serializable {
    String rowKey;
    String value;

    @Override
    public boolean equals(Object obj) {
        return obj instanceof WGCell &&
                ((WGCell) obj).rowKey.equals(this.rowKey);
    }

    public WGCell(String rowKey) {
        this.rowKey = rowKey;
    }

    public WGCell(String rowKey, String value) {
        this.rowKey = rowKey;
        this.value = value;
    }
}

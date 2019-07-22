package in.nimbo.queue;

import java.io.Closeable;

public interface LinkQueue extends Closeable {
    void put(String link);
    String pop();
}

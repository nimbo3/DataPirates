package in.nimbo.cache;

import java.io.Closeable;

public interface VisitedLinksCache extends Closeable {
    void put(String normalizedUrl);
    boolean hasVisited(String normalizedUrl);
}

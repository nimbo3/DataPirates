package in.nimbo.util;

public interface VisitedLinksCache {
    void put(String normalizedUrl);
    boolean hasVisited(String normalizedUrl);
}

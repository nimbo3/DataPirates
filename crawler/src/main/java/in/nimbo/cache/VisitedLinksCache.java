package in.nimbo.cache;

public interface VisitedLinksCache {
    void put(String normalizedUrl);

    boolean hasVisited(String normalizedUrl);
}

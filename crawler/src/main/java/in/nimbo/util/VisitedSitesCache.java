package in.nimbo.util;

public interface VisitedSitesCache {
    void put(String normalizedUrl);
    boolean hasVisited(String normalizedUrl);
}

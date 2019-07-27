package in.nimbo.database;

import in.nimbo.model.SearchResult;

import java.util.List;

public interface Searchable {
    List<SearchResult> search (String text);
}

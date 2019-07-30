package in.nimbo;

import in.nimbo.model.SearchResult;

import java.util.List;


public interface Searchable {
    List<SearchResult> search(String input);
}

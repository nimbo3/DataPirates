package in.nimbo.fetch;

import in.nimbo.exception.FetchException;
import in.nimbo.model.Pair;


public interface Fetcher {

    Pair<String, String> fetch(String url) throws FetchException;

}

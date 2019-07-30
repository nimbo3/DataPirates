package in.nimbo;

import in.nimbo.exception.FetchException;

public interface Fetcher {

    String fetch(String url) throws FetchException;
}

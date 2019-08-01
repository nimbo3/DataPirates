package in.nimbo.fetch;

import in.nimbo.exception.FetchException;


public interface Fetcher {

    String fetch(String url) throws FetchException;

    boolean isContentTypeTextHtml();

    String getRedirectUrl();
}
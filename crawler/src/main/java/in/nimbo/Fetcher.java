package in.nimbo;

import org.apache.http.client.RedirectException;

import java.io.IOException;

public interface Fetcher {

    String fetch(String url) throws IOException, RedirectException;
}

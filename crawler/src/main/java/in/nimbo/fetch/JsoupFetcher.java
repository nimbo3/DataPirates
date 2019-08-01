package in.nimbo.fetch;

import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import in.nimbo.exception.FetchException;
import in.nimbo.fetch.Fetcher;
import org.jsoup.Connection;
import org.jsoup.Jsoup;

import java.io.IOException;

public class JsoupFetcher implements Fetcher {
    private Timer fetchTimer = SharedMetricRegistries.getDefault().timer("fetcher");
    private String redirectUrl;

    @Override
    public String fetch(String url) throws FetchException {
        try (Timer.Context time = fetchTimer.time()) {
            Connection.Response response = Jsoup.connect(url)
                    .followRedirects(true)
                    .timeout(30000)
                    .execute();
            redirectUrl = response.url().toString();
            return response.body();
        } catch (IOException e) {
            throw new FetchException("can't fetch url: " + url, e);
        }
    }

    @Override
    public boolean isContentTypeTextHtml() {
        return true;
    }

    public String getRedirectUrl() {
        return redirectUrl;
    }
}

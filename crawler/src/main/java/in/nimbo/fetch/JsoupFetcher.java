package in.nimbo.fetch;

import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import in.nimbo.exception.FetchException;
import in.nimbo.fetch.Fetcher;
import org.jsoup.Jsoup;

import java.io.IOException;

public class JsoupFetcher implements Fetcher {
    private Timer fetchTimer = SharedMetricRegistries.getDefault().timer("fetcher");

    @Override
    public String fetch(String url) throws FetchException {
        try (Timer.Context time = fetchTimer.time()) {
            return Jsoup.connect(url)
                    .followRedirects(true)
                    .timeout(30000)
                    .get().toString();
        } catch (IOException e) {
            throw new FetchException("can't fetch url: " + url, e);
        }
    }

    @Override
    public boolean isContentTypeTextHtml() {
        return true;
    }
}

package in.nimbo;

import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import org.jsoup.Jsoup;

import java.io.IOException;

public class JsoupFetcher implements Fetcher {
    private Timer fetchTimer = SharedMetricRegistries.getDefault().timer("fetcher");

    @Override
    public String fetch(String url) throws IOException {
        try (Timer.Context time = fetchTimer.time()) {
            return Jsoup.connect(url)
                    .followRedirects(true)
                    .timeout(30000)
                    .get().toString();
        }
    }

    @Override
    public boolean isContentTypeTextHtml() {
        return true;
    }
}

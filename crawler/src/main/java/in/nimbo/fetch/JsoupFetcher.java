package in.nimbo.fetch;

import com.codahale.metrics.Meter;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import in.nimbo.exception.FetchException;
import org.apache.log4j.Logger;
import org.jsoup.Connection;
import org.jsoup.Jsoup;

import java.io.IOException;

public class JsoupFetcher implements Fetcher {
    private Logger logger = Logger.getLogger(JsoupFetcher.class);
    private Timer fetchTimer;
    private Meter successFetchMeter;
    private String redirectedUrl;

    public JsoupFetcher() {
        fetchTimer = SharedMetricRegistries.getDefault().timer("fetcher");
        successFetchMeter = SharedMetricRegistries.getDefault().meter("fetcher successful");
    }

    @Override
    public String fetch(String url) throws FetchException {
        try (Timer.Context time = fetchTimer.time()) {
            logger.trace(String.format("Trying to fetch Url [%s]", url));
            Connection.Response response = Jsoup.connect(url)
                    .followRedirects(true)
                    .timeout(30000)
                    .execute();
            redirectedUrl = response.url().toString();
            successFetchMeter.mark();
            logger.trace(String.format("Url [%s] fetched successfully", url));
            return response.body();
        } catch (IOException e) {
            throw new FetchException(String.format("Can't fetch url [%s]", url), e);
        }
    }

    public String getRedirectedUrl() {
        return redirectedUrl;
    }
}

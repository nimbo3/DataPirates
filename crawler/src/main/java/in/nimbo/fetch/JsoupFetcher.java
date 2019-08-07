package in.nimbo.fetch;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.typesafe.config.Config;
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
    private int connectionTimeOut;

    public JsoupFetcher(Config config) {
        connectionTimeOut = config.getInt("fetcher.connection.timeout.milliseconds");
        MetricRegistry metricRegistry = SharedMetricRegistries.getDefault();
        fetchTimer = metricRegistry.timer("fetcher");
        successFetchMeter = metricRegistry.meter("fetcher successful");
    }

    @Override
    public String fetch(String url) throws FetchException {
        try (Timer.Context time = fetchTimer.time()) {
            logger.trace(String.format("Trying to fetch Url [%s]", url));
            Connection.Response response = Jsoup.connect(url)
                    .followRedirects(true)
                    .timeout(connectionTimeOut)
                    .execute();
            redirectedUrl = response.url().toString();
            logger.trace(String.format("Url [%s] fetched successfully", url));
            successFetchMeter.mark();
            return response.body();
        } catch (IOException e) {
            throw new FetchException(String.format("Can't fetch url [%s]", url), e);
        }
    }

    public String getRedirectedUrl() {
        return redirectedUrl;
    }
}

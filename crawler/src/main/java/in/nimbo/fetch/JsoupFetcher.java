package in.nimbo.fetch;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.typesafe.config.Config;
import in.nimbo.exception.FetchException;
import in.nimbo.model.Pair;
import org.apache.log4j.Logger;
import org.jsoup.Connection;
import org.jsoup.HttpStatusException;
import org.jsoup.Jsoup;
import org.jsoup.UnsupportedMimeTypeException;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;

public class JsoupFetcher implements Fetcher {
    private Logger logger = Logger.getLogger(JsoupFetcher.class);
    private Timer fetchTimer;
    private Meter successFetchMeter;
    private Meter malformedURLMeter;
    private Meter socketTimeoutMeter;
    private Meter httpStatusMeter;
    private Meter unsupportedMimeTypeMeter;
    private int connectionTimeOut;

    public JsoupFetcher(Config config) {
        connectionTimeOut = config.getInt("fetcher.connection.timeout.milliseconds");
        MetricRegistry metricRegistry = SharedMetricRegistries.getDefault();
        fetchTimer = metricRegistry.timer("fetcher");
        successFetchMeter = metricRegistry.meter("fetcher successful");
        malformedURLMeter = metricRegistry.meter("fetcher malformed url");
        socketTimeoutMeter = metricRegistry.meter("fetcher socket timeout");
        httpStatusMeter = metricRegistry.meter("fetcher http status error");
        unsupportedMimeTypeMeter = metricRegistry.meter("fetcher unsupported mime type");
    }

    @Override
    public Pair<String, String> fetch(String url) throws FetchException {
        try (Timer.Context time = fetchTimer.time()) {
            logger.trace(String.format("Trying to fetch Url [%s]", url));
            Connection.Response response = Jsoup.connect(url)
                    .followRedirects(true)
                    .timeout(connectionTimeOut)
                    .execute();
            String redirectedUrl = response.url().toString();
            String html = response.body();
            logger.trace(String.format("Url [%s] fetched successfully", url));
            successFetchMeter.mark();
            return new Pair<>(redirectedUrl, html);
        } catch (MalformedURLException e) {
            malformedURLMeter.mark();
            throw new FetchException(String.format("Can't fetch url [%s]", url), e);
        } catch (SocketTimeoutException e) {
            socketTimeoutMeter.mark();
            throw new FetchException(String.format("Can't fetch url [%s]", url), e);
        } catch (HttpStatusException e) {
            httpStatusMeter.mark();
            throw new FetchException(String.format("Can't fetch url [%s]", url), e);
        } catch (UnsupportedMimeTypeException e) {
            unsupportedMimeTypeMeter.mark();
            throw new FetchException(String.format("Can't fetch url [%s]", url), e);
        } catch (IOException e) {
            throw new FetchException(String.format("Can't fetch url [%s]", url), e);
        }
    }

}

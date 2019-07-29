
package in.nimbo;

import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.typesafe.config.Config;
import in.nimbo.exception.FetchException;
import org.apache.commons.httpclient.RedirectException;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIUtils;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.LaxRedirectStrategy;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.List;

public class FetcherImpl implements Fetcher {
    private static final Logger logger = LoggerFactory.getLogger(FetcherImpl.class);
    private static final String DEFAULT_ACCEPT_LANGUAGE = "en-us,en-gb,en;q=0.7,*;q=0.3";
    private static final String DEFAULT_ACCEPT = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8";
    private static final String DEFAULT_ACCEPT_CHARSET = "utf-8,ISO-8859-1;q=0.7,*;q=0.7";
    private static final String DEFAULT_ACCEPT_ENCODING = "x-gzip, gzip";
    private Timer fetchTimer = SharedMetricRegistries.getDefault().timer("fetcher");
    private HttpClient client;
    private String rawHtmlDocument;
    private int responseStatusCode;
    private ContentType contentType;
    private String redirectUrl;
    private Config config;

    public FetcherImpl(Config config) {
        this.config = config;
        init();
    }

    public String getRedirectUrl() {
        return redirectUrl;
    }

    void init() {
        /*
         TODO: 7/22/19
         - handle or deactivate redirects
         - disable ssl
         - handle status codes (for example too many requests)
         - test redirects
         */

        int connectionTimeout = config.getInt("fetcher.connection.timeout.milliseconds");
        int maxRedirects = config.getInt("fetcher.max.redirects");
        int maxTotalConnections = config.getInt("fetcher.client.num.of.maximum.total.connections");
        int maxConnectionsPerRoute = config.getInt("fetcher.client.num.of.maximum.connections.per.route");

        HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
        httpClientBuilder.setRedirectStrategy(new LaxRedirectStrategy());

        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(connectionTimeout)
                .setConnectionRequestTimeout(connectionTimeout)
                .setSocketTimeout(connectionTimeout)
                .setMaxRedirects(maxRedirects)
                .setRedirectsEnabled(true)
                .build();
        httpClientBuilder.setDefaultRequestConfig(requestConfig);

        // to handle multithreading we're using PoolingHttpClientConnectionManager
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setDefaultMaxPerRoute(maxConnectionsPerRoute);
        connectionManager.setMaxTotal(maxTotalConnections);
        httpClientBuilder.setConnectionManager(connectionManager);


        HashSet<Header> defaultHeaders = new HashSet<>();
        defaultHeaders.add(new BasicHeader(HttpHeaders.ACCEPT_LANGUAGE, DEFAULT_ACCEPT_LANGUAGE));
        defaultHeaders.add(new BasicHeader(HttpHeaders.ACCEPT_CHARSET, DEFAULT_ACCEPT_CHARSET));
        defaultHeaders.add(new BasicHeader(HttpHeaders.ACCEPT_ENCODING, DEFAULT_ACCEPT_ENCODING));
        defaultHeaders.add(new BasicHeader(HttpHeaders.ACCEPT, DEFAULT_ACCEPT));

        httpClientBuilder.setDefaultHeaders(defaultHeaders);

        client = httpClientBuilder.build();
    }

    @Override
    public String fetch(String url) throws IOException, FetchException {
        try (Timer.Context time = fetchTimer.time()) {
            HttpClientContext context = HttpClientContext.create();
            HttpGet httpGet = new HttpGet(url);
            try (CloseableHttpResponse response = (CloseableHttpResponse) client.execute(httpGet, context)){
                HttpHost target = context.getTargetHost();
                List<URI> redirectLocations = context.getRedirectLocations();
                URI location = URIUtils.resolve(httpGet.getURI(), target, redirectLocations);
                redirectUrl = location.toASCIIString();
                responseStatusCode = response.getStatusLine().getStatusCode();
                rawHtmlDocument = EntityUtils.toString(response.getEntity());
                contentType = ContentType.getOrDefault(response.getEntity());
            } catch (URISyntaxException e) {
                throw new FetchException(String.format("uri syntax exception in fetching %s", url), e);
            } catch (RedirectException e) {
                throw new FetchException(String.format("Redirect exception in fetching %s", url), e);
            } catch (ClientProtocolException e) {
                throw new FetchException(String.format("ClientProtocolException in fetching %s", url), e);
            }
        } catch (IllegalArgumentException e) {
            logger.error("IllegalArgumentException In Fetcher", e);
        }
        return rawHtmlDocument;
    }

    public boolean isContentTypeTextHtml() {
        return contentType.getMimeType().equals(ContentType.TEXT_HTML.getMimeType());
    }
}
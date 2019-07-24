package in.nimbo;

import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.RedirectException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.HashSet;

public class FetcherImpl implements Fetcher {
    private static final String DEFAULT_ACCEPT_LANGUAGE = "en-us,en-gb,en;q=0.7,*;q=0.3";
    private static final String DEFAULT_ACCEPT = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8";
    private static final String DEFAULT_ACCEPT_CHARSET = "utf-8,ISO-8859-1;q=0.7,*;q=0.7";
    private static final String DEFAULT_ACCEPT_ENCODING = "x-gzip, gzip";

    private HttpClient client;
    private String rawHtmlDocument;
    private int responseStatusCode;
    private ContentType contentType;

    public FetcherImpl() {
        init();
    }

    void init() {
        /*
         TODO: 7/22/19
         - handle or deactivate redirects
         - create headers
         - disable ssl
         */

        HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();

        // to handle multithreading we're using PoolingHttpClientConnectionManager
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setDefaultMaxPerRoute(1);
        connectionManager.setMaxTotal(500);
        httpClientBuilder.setConnectionManager(connectionManager);

        RequestConfig.Builder requestConfigBuilder = RequestConfig.custom();
        requestConfigBuilder.setRedirectsEnabled(false);
        httpClientBuilder.setDefaultRequestConfig(requestConfigBuilder.build());

        HashSet<Header> defaultHeaders = new HashSet<Header>();
        defaultHeaders.add(new BasicHeader(HttpHeaders.ACCEPT_LANGUAGE, DEFAULT_ACCEPT_LANGUAGE));
        defaultHeaders.add(new BasicHeader(HttpHeaders.ACCEPT_CHARSET, DEFAULT_ACCEPT_CHARSET));
        defaultHeaders.add(new BasicHeader(HttpHeaders.ACCEPT_ENCODING, DEFAULT_ACCEPT_ENCODING));
        defaultHeaders.add(new BasicHeader(HttpHeaders.ACCEPT, DEFAULT_ACCEPT));

        httpClientBuilder.setDefaultHeaders(defaultHeaders);
        httpClientBuilder.disableRedirectHandling();


        client = httpClientBuilder.build();
    }

    @Override
    public String fetch(String url) throws IOException, RedirectException {

        CloseableHttpResponse response = (CloseableHttpResponse) client.execute(new HttpGet(url), HttpClientContext.create());
        try {
            responseStatusCode = response.getStatusLine().getStatusCode();
            rawHtmlDocument = EntityUtils.toString(response.getEntity());
            contentType = ContentType.getOrDefault(response.getEntity());
        } finally {
            // maybe response type to be closeable and closing it should be optional
            response.close();
        }
        // TODO: 7/23/19 bad smell in hard coding !!
        if (responseStatusCode >= 300 && responseStatusCode < 400) // checks if it has been redirected or not
            throw new RedirectException("url redirection occurred!");
        return rawHtmlDocument;
    }

    boolean isContentTypeTextHtml() {
        return contentType == ContentType.TEXT_HTML;
    }

    public String getRawHtmlDocument() {
        return rawHtmlDocument;
    }
}

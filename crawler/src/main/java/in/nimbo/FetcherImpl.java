package in.nimbo;

import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

public class FetcherImpl implements Fetcher {
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

        client = httpClientBuilder.build();
    }

    @Override
    public String fetch(String url) throws IOException {

        CloseableHttpResponse response = (CloseableHttpResponse) client.execute(new HttpGet(url), HttpClientContext.create());
        try {
            responseStatusCode = response.getStatusLine().getStatusCode();
            rawHtmlDocument = EntityUtils.toString(response.getEntity());
            contentType = ContentType.getOrDefault(response.getEntity());
        } finally {
            // maybe response type to be closeable and closing it should be optional
            response.close();
        }
        return rawHtmlDocument;
    }

    boolean isContentTypeTextHtml() {
        return contentType == ContentType.TEXT_HTML;
    }
}

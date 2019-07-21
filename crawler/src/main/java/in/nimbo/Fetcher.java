package in.nimbo;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

public class Fetcher {
    private String url;
    private String rawHtmlDocument;
    private int responseStatusCode;
    private ContentType contentType;

    public Fetcher(String url) {
        this.url = url;
    }

    void fetch() throws IOException {
        CloseableHttpClient client = HttpClientBuilder.create().build();
        CloseableHttpResponse response = client.execute(new HttpGet(url));

        responseStatusCode = response.getStatusLine().getStatusCode();
        rawHtmlDocument = EntityUtils.toString(response.getEntity());
        contentType =  ContentType.getOrDefault(response.getEntity());
    }

    boolean isContentTypeTextHtml() {
        return contentType == ContentType.TEXT_HTML;
    }
}

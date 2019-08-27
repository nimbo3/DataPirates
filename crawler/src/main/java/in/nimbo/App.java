package in.nimbo;

import com.codahale.metrics.SharedMetricRegistries;

public class App {
    public static void main(String[] args) {
        SharedMetricRegistries.setDefault("data-pirates-crawler");
        Crawler crawler = new Crawler();
        crawler.start();
    }
}

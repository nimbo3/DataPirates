package in.nimbo;

import java.io.IOException;

public class App {
    public static void main(String[] args) {
        FetcherImpl fetcher = new FetcherImpl();
        int constant = 1000;
        crawlerThread[] threads = new crawlerThread[constant];
        for (int i = 0; i < constant; i++) {
            threads[i] = new crawlerThread(fetcher);
        }
        for (int i = 0; i < constant; i++) {
            threads[i].start();
        }
    }
}

class crawlerThread extends Thread {
    FetcherImpl fetcher;

    public crawlerThread(FetcherImpl fetcher) {
        this.fetcher = fetcher;
    }

    @Override
    public void run() {
        boolean quit = false;
        while (!quit) {
            // TODO: 7/22/19 get url from Kafka
            String url = "https://tabnak.ir";
            // TODO: 7/22/19 check if url has been checked recently
            try {
                fetcher.fetch(url);
            } catch (IOException e) {
                // TODO: 7/22/19 do something in here
                e.printStackTrace();
            }
            // TODO: 7/22/19 parse and insert into databases
        }
    }
}

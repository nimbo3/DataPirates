package in.nimbo.shutdown_hook;

import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public class ShutdownHook extends Thread {
    private static Logger logger = LoggerFactory.getLogger(ShutdownHook.class);
    private final Config config;
    private Timer shutDownTimer = SharedMetricRegistries.getDefault().timer("shutdown");


    private List<Closeable> closeables;

    public ShutdownHook(List<Closeable> closeables, Config config) {
        this.config = config;
        this.closeables = closeables;
    }

    public void run() {
        try (Timer.Context time = shutDownTimer.time()) {
            logger.info("Shutdown hook thread initiated.");
            for (Closeable closeable : closeables) {
                try {
                    closeable.close();
                } catch (IOException e) {
                    logger.error("Shutdown hook can't close object with name: " + closeable.getClass().getSimpleName(), e);
                }
            }
        }
    }
}
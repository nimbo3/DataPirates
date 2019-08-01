package in.nimbo;

import com.codahale.metrics.SharedMetricRegistries;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class App {

    public static void main(String[] args) {
        Config config = ConfigFactory.load("config");
        SharedMetricRegistries.setDefault(config.getString("metric.registry.name"));
        SpringApplication.run(App.class, args);
    }
}


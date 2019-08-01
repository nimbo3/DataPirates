package in.nimbo;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.jmx.JmxReporter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class App {

    public static void main(String[] args) {
        Config config = ConfigFactory.load("config");
        SharedMetricRegistries.setDefault(config.getString("metric.registry.name"));
        MetricRegistry metricRegistry = SharedMetricRegistries.getDefault();
        JmxReporter jmxReporter = JmxReporter.forRegistry(metricRegistry).inDomain(config.getString("metric.domain.name")).build();
        jmxReporter.start();
        SpringApplication.run(App.class, args);
    }
}


package in.nimbo;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.jmx.JmxReporter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import in.nimbo.model.Hbase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;

@SpringBootApplication
public class App {

    public static void main(String[] args) {
        try {
            Config config = ConfigFactory.load("config");
            Configuration hbaseConfig = HBaseConfiguration.create();
            Connection hbaseConnection = ConnectionFactory.createConnection(hbaseConfig);
            Hbase hbase = new Hbase(hbaseConnection, config);
            SearchResultController.setHbase(hbase);
            SharedMetricRegistries.setDefault(config.getString("metric.registry.name"));
            MetricRegistry metricRegistry = SharedMetricRegistries.getDefault();
            JmxReporter jmxReporter = JmxReporter.forRegistry(metricRegistry).inDomain(config.getString("metric.domain.name")).build();
            jmxReporter.start();
            SpringApplication.run(App.class, args);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}


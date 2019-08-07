package in.nimbo.kafka;

import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.typesafe.config.Config;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.Closeable;
import java.util.Properties;

public class LinkProducer implements Closeable {
    private Timer sendTimer = SharedMetricRegistries.getDefault().timer("kafka-sending");
    private KafkaProducer<String, String> producer;
    private String topicName;

    public LinkProducer(Config config) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", config.getString("kafka.bootstrap.servers"));
        properties.put("acks", config.getString("kafka.acks"));
        properties.put("key.serializer", config.getString("kafka.key.serializer"));
        properties.put("value.serializer", config.getString("kafka.value.serializer"));
        this.producer = new KafkaProducer<>(properties);
        this.topicName = config.getString("kafka.topic.name");
    }

    public void send(String link) {
        try (Timer.Context time = sendTimer.time()) {
            producer.send(new ProducerRecord<>(topicName, link));
        }
    }

    @Override
    public void close() {
        producer.close();
    }
}

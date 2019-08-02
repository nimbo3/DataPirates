package in.nimbo.kafka;

import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.typesafe.config.Config;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;

public class LinkConsumer implements Closeable {
    private final Config config;
    private Timer receiveTimer = SharedMetricRegistries.getDefault().timer("kafka-receiving");
    private ArrayBlockingQueue<String> buffer;
    private KafkaConsumer<String, String> consumer;
    private String topicName;
    private boolean closed = false;
    private Thread kafkaReaderThread;

    public LinkConsumer(Config config) {
        this.config = config;
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", config.getString("kafka.bootstrap.servers"));
        properties.setProperty("group.id", config.getString("kafka.group.id"));
        properties.setProperty("enable.auto.commit", config.getString("kafka.enable.auto.commit"));
        properties.setProperty("auto.commit.interval.ms", config.getString("kafka.auto.commit.interval.ms"));
        properties.setProperty("key.deserializer", config.getString("kafka.key.deserializer"));
        properties.setProperty("value.deserializer", config.getString("kafka.value.deserializer"));
        this.consumer = new KafkaConsumer<>(properties);
        topicName = config.getString("kafka.topic.name");
        buffer = new ArrayBlockingQueue<>(config.getInt("kafka.buffer.size"));
    }

    public void start() {
        if (kafkaReaderThread == null) {
            kafkaReaderThread = new KafkaReaderThread();
            kafkaReaderThread.start();
        }
    }

    public String pop() throws InterruptedException {
        return buffer.take();
    }

    @Override
    public void close() {
        closed = true;
    }

    private class KafkaReaderThread extends Thread {
        @Override
        public void run() {
            final int KAFKA_CONSUME_POLL_TIMEOUT = config.getInt("kafka.consume.poll.timeout");
            consumer.subscribe(Arrays.asList(topicName));
            while (!closed) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(KAFKA_CONSUME_POLL_TIMEOUT));
                    for (ConsumerRecord<String, String> record : records)
                        buffer.put(record.value());
                    consumer.commitAsync();
                } catch (InterruptedException e) {
                    closed = true;
                    consumer.commitSync();
                }
            }
        }
    }
}

package in.nimbo.kafka;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.typesafe.config.Config;
import in.nimbo.FetcherThread;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;

public class LinkConsumer implements Closeable {
    private final Config config;
    private Logger logger = LoggerFactory.getLogger(LinkConsumer.class);
    private Meter pollLinksMeter = SharedMetricRegistries.getDefault().meter("kafka-poll-links");
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
        SharedMetricRegistries.getDefault().register(
                MetricRegistry.name(LinkConsumer.class, "kafka consumer buffer queue"),
                (Gauge<Integer>) buffer::size);
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
            consumer.subscribe(Collections.singletonList(topicName));
            try {
                while (!closed) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(KAFKA_CONSUME_POLL_TIMEOUT));
                    logger.trace(String.format("[%d] New links consumed from kafka.", records.count()));
                    pollLinksMeter.mark(records.count());
                    for (ConsumerRecord<String, String> record : records)
                        buffer.put(record.value());
                    consumer.commitAsync();
                }
            } catch (InterruptedException e) {
                logger.error("Kafka reader thread interrupted");
                consumer.commitSync();
                Thread.currentThread().interrupt();
            }
        }
    }
}

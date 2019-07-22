package in.nimbo.util;

import com.typesafe.config.Config;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;

public class LinkConsumer implements Closeable {
    private ArrayBlockingQueue<String> buffer;
    private KafkaConsumer<String, String> consumer;
    private String topicName;
    private boolean closed = false;
    private Thread kafkaReaderThread;

    public LinkConsumer(KafkaConsumer<String, String> consumer, Config config) {
        this.consumer = consumer;
        topicName = config.getString("topic.Name");
        buffer = new ArrayBlockingQueue<>(config.getInt("buffer.size"));
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
            consumer.subscribe(Arrays.asList(topicName));
            while (!closed) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records)
                        buffer.put(record.value());
                    consumer.commitAsync();
                } catch (InterruptedException e) {
                    closed = true;
                    consumer.commitSync();
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}

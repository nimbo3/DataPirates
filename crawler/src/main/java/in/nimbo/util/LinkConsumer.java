package in.nimbo.util;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.Closeable;
import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;

public class LinkConsumer implements Closeable {
    private ArrayBlockingQueue<String> buffer = new ArrayBlockingQueue<>(100);
    private KafkaConsumer<String, String> consumer;
    private boolean closed = false;
    private Thread kafkaReaderThread;

    public LinkConsumer(KafkaConsumer<String, String> consumer) {
        this.consumer = consumer;
    }

    public void start() {
        if (kafkaReaderThread != null) {
            kafkaReaderThread = new KafkaReaderThread();
            kafkaReaderThread.start();
        }
    }

    public String pop() throws InterruptedException {
        return buffer.take();
    }

    @Override
    public void close() {
        kafkaReaderThread.interrupt();
    }

    private class KafkaReaderThread extends Thread {
        @Override
        public void run() {
            while (!closed) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records)
                        buffer.put(record.value());
                } catch (InterruptedException e) {
                    closed = true;
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}

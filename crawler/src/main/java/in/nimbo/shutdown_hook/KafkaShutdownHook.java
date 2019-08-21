package in.nimbo.shutdown_hook;

import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import in.nimbo.kafka.LinkConsumer;
import in.nimbo.kafka.LinkProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaShutdownHook extends Thread {
    private static Logger logger = LoggerFactory.getLogger(KafkaShutdownHook.class);
    private Timer kafkaShutdownTimer = SharedMetricRegistries.getDefault().timer("kafka-shutdown");
    private LinkConsumer linkConsumer;
    private LinkProducer linkProducer;


    public KafkaShutdownHook(LinkConsumer linkConsumer, LinkProducer linkProducer) {
        this.linkConsumer = linkConsumer;
        this.linkProducer = linkProducer;
    }

    @Override
    public void run() {
        try (Timer.Context time = kafkaShutdownTimer.time()) {
            logger.info("Kafka Shutdown hook started ...");
            linkConsumer.close();
            while (linkConsumer.peek() != null)
                linkProducer.send(linkConsumer.pop());
            linkProducer.close();
            logger.info("Kafka Shutdown hook completed.");
        }
    }
}

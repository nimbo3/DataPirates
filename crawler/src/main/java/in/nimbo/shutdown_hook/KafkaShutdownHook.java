package in.nimbo.shutdown_hook;

import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.typesafe.config.Config;
import in.nimbo.kafka.LinkConsumer;
import in.nimbo.kafka.LinkProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaShutdownHook extends Thread {
    private static Logger logger = LoggerFactory.getLogger(KafkaShutdownHook.class);
    private final Config config;
    private Timer kafkaShutdownTimer = SharedMetricRegistries.getDefault().timer("kafka-shutdown");
    private LinkConsumer linkConsumer;
    private LinkProducer linkProducer;


    public KafkaShutdownHook(LinkConsumer linkConsumer, LinkProducer linkProducer, Config config) {
        this.config = config;
        this.linkConsumer = linkConsumer;
        this.linkProducer = linkProducer;
    }

    @Override
    public void run() {
        try (Timer.Context time = kafkaShutdownTimer.time()) {
            logger.info("KafkaShutdown hook thread initiated.");
            linkConsumer.close();
            //TODO do sth to return back consumed files to kafka
            linkProducer.close();
        }
    }
}

package dev.othmanfr.consumer;

import dev.othmanfr.producer.ProducerDemoWithCallback;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoCooperative {
    private static Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Start consuming event");
        Properties properties = new Properties();
        String groupId = "consumer-demo";
        String topic = "demo";
        // broker properties
        properties.setProperty("bootstrap.servers", "127.0.0.1:9091");
        // producer properties
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        /**
         * The auto offset reset consumer configuration defines how a consumer
         * should behave when consuming from a topic partition when there is no initial offset
         *  none : throw an exception if no offest present for the consumer group.
         *  earliest : Reset offest to earliest offest. Consume from the beginning of the topic partition
         *  latest : Reset offest to latest offest. Consume from the end of the topic partition (Default)
         */
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());

        // create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detecting a shutdown, let's exit by calling consumer.wakeup()");
                consumer.wakeup();
                // join the main thread to allow execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        // subscribe to a topic
        consumer.subscribe(Arrays.asList(topic));
        try {
            // poll for data
            while (true) {
                log.info("Start Polling");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    log.info(String.format(" key : %s | value : %s", record.key(), record.value()));
                    log.info(String.format(" Partition : %s | Offset : %s", record.partition(), record.offset()));
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer start to shutdown.");
        } catch (Exception e) {
            log.error("Unexpected exception in consumer {}.", e.getMessage());
        } finally {
            consumer.close(); // close the consumer, this will also commit the offsets
            log.info("Consumer is gracefully shutdown.");
        }

    }
}

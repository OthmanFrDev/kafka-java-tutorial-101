package dev.othmanfr.consumer;

import dev.othmanfr.producer.ProducerDemoWithCallback;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    private static Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Start consuming event");
        Properties properties = new Properties();
        String groupId = "consumer-demo";
        String topic="demo";
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

        // create a consumer
        KafkaConsumer<String,String> consumer= new KafkaConsumer<String, String>(properties);

        // subscribe to a topic
        consumer.subscribe(Arrays.asList(topic));

        // poll for data
        while(true){
            log.info("Start Polling");
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(50000));
            for (ConsumerRecord<String, String> record : records) {
                log.info(String.format(" key : %s | value : %s",record.key(),record.value()));
                log.info(String.format(" Partition : %s | Offset : %s",record.partition(),record.offset()));
            }
        }
    }
}

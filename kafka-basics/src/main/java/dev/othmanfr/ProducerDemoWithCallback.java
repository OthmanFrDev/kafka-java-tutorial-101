package dev.othmanfr;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    private static Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Start sending event");
        Properties properties = new Properties();
        // broker properties
        properties.setProperty("bootstrap.servers", "127.0.0.1:9091");
        // producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        // create a producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        // create a producer record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo","Hello world");
        // send data
        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    throw new RuntimeException(exception.getMessage());
                } else {
                    log.info("Event sended successfully to the partition {} in topic {}", metadata.partition(), metadata.topic());
                }
            }
        });
        // tell the producer to send all data and block until done -- synchronous
        producer.flush();
        // flush and close the producer
        producer.close();
        log.info("Finish sending event");
    }
}

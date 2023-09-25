package io.conduktor.demo.kafka;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
 import java.util.List;
import java.util.Properties;

public class ConsumerDemo {

    public static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka consumer");
        String groupId = "my-java-application";
        String topic = "demo_topic";
        final Properties properties = new Properties();

        // connect to conduktor playground
        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"4hfzkn9qIEiByakitGauSP\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiI0aGZ6a245cUlFaUJ5YWtpdEdhdVNQIiwib3JnYW5pemF0aW9uSWQiOjc2NzI3LCJ1c2VySWQiOjg5MjY2LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiJlOWZlMTBmMi1hNWE2LTQ5OTItOGE5ZS1hOTE3NjNhYmJkYmMifX0.cjRKIjskjJcDuqQXqJCp9KGzNQxv1EEgtW0WH9UrLdI\";");
        properties.setProperty("sasl.mechanism", "PLAIN");

        // create consumer config
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", groupId);
        // For the next property there are 3 values
        // none | earliest | latest
        //      | read from the beginning of our topic | rad from this point in time
        properties.setProperty("auto.offset.reset", "earliest");

        // create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // subscribe to a topic
        consumer.subscribe(List.of(topic));

        // poll for data
        while (true) {
            log.info("Pulling data");
            ConsumerRecords<String, String> consumerRecords =
                    consumer.poll(Duration.ofMillis(1000));

            for(ConsumerRecord<String, String> consumerRecord: consumerRecords) {
                log.info("Key: " + consumerRecord.key() + " | Value: " + consumerRecord.value());
                log.info("Partition: " + consumerRecord.partition() + " | Offset: " + consumerRecord.offset());
            }
        }

    }
}

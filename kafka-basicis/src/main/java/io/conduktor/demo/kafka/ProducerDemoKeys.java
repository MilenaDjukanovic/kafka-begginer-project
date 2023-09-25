package io.conduktor.demo.kafka;


import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {

    public static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka producer");
        final Properties properties = new Properties();

        // connect to conduktor playground
        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"4hfzkn9qIEiByakitGauSP\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiI0aGZ6a245cUlFaUJ5YWtpdEdhdVNQIiwib3JnYW5pemF0aW9uSWQiOjc2NzI3LCJ1c2VySWQiOjg5MjY2LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiJlOWZlMTBmMi1hNWE2LTQ5OTItOGE5ZS1hOTE3NjNhYmJkYmMifX0.cjRKIjskjJcDuqQXqJCp9KGzNQxv1EEgtW0WH9UrLdI\";");
        properties.setProperty("sasl.mechanism", "PLAIN");

        // set the producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for(int j = 0; j < 2; j++) {
            for (int i = 0; i < 10; i++) {
                // create a producer record
                String topic = "demo_topic";
                String key = "id_" + i;
                String value = "Hello world! " + i;
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(topic, key, value);

                // send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        // executed everytime a record is successfully sent or an exception is thrown
                        if (exception == null) {
                            // the record is successfully sent
                            log.info("Key: " + key + " | Partition: " + metadata.partition());
                        } else {
                            log.error("Error while producing", exception);
                        }
                    }
                });
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        // flush the producer
        // tell the producer to send all data and block until done - synchronous
        producer.flush();

        // flushes and closes the producers
        producer.close();
    }
}

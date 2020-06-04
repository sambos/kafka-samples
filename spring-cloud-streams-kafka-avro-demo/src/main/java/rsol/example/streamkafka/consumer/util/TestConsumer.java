package rsol.example.streamkafka.consumer.util;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;

public class TestConsumer {

    //public static final String TOPIC_NAME = "employee-details";
    public static final String TOPIC_NAME = "postgres-movies";
    
    public static void main(String[] args) {

        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "demo-group");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
        consumerProperties.put("schema.registry.url", "http://localhost:8081");

        KafkaConsumer<Long, GenericRecord> kafkaConsumer = new KafkaConsumer<>(consumerProperties);
        kafkaConsumer.subscribe(Collections.singletonList(TOPIC_NAME));

        try {
            while (true) {
                ConsumerRecords<Long, GenericRecord> records = kafkaConsumer.poll(Duration.ofMillis(3000));
                System.out.println("Fetched " + records.count() + " records");
                for (ConsumerRecord<Long, GenericRecord> record : records) {
                	System.out.println(record.toString());
                    System.out.println("Received: " + record.key() + ":" + record.value());
                    record.headers().forEach(header -> {
                        System.out.println("Header key: " + header.key() + ", value:" + header.value());
                    });
                }
                kafkaConsumer.commitSync();
                TimeUnit.SECONDS.sleep(5);
            }
        }catch (Exception e) {
            e.printStackTrace();
        } finally {
            kafkaConsumer.close();
        }
    }
}

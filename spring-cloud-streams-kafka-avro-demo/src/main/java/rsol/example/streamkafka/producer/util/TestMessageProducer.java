package rsol.example.streamkafka.producer.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class TestMessageProducer {

    public static final String TOPIC_NAME = "employee-details";

    public static void main(String[] args) throws Exception {

        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProperties.put("schema.registry.url", "http://localhost:8081");

        Producer<Long, String> producer = new KafkaProducer<>(producerProperties);

        try {
            Long counter = 1L;
            while (true) {
                Headers headers = new RecordHeaders();
                headers.add(new RecordHeader("header-1", "header-value-1".getBytes()));
                headers.add(new RecordHeader("header-2", "header-value-2".getBytes()));
                ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(TOPIC_NAME, null, counter, "A sample message", headers);

                producer.send(record);
                System.out.println("Send record#"+counter);
                counter++;
                TimeUnit.SECONDS.sleep(3);
            }
        } finally {
            producer.close();
        }
    }
}

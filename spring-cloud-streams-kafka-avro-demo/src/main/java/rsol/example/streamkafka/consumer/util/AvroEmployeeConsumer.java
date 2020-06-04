package rsol.example.streamkafka.consumer.util;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.stream.IntStream;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import rsol.example.streamkafka.schema.Employee;

public class AvroEmployeeConsumer {

    private final static String BOOTSTRAP_SERVERS = "localhost:29092";
    private final static String TOPIC = "new-employees";

    private static Consumer<Long, Object> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleAvroConsumer2");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                LongDeserializer.class.getName());

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //Use Kafka Avro Deserializer.
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                KafkaAvroDeserializer.class.getName());  //<----------------------
        
        //Use Specific Record or else you get Avro GenericRecord.
        //props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");


        //Schema registry location.
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
                "http://localhost:8081"); //<----- Run Schema Registry on 8081


        return new KafkaConsumer<>(props);
    }





    public static void main(String... args) {

        final Consumer<Long, Object> consumer = createConsumer();
        consumer.subscribe(Collections.singletonList(TOPIC));

        IntStream.range(1, 5).forEach(index -> {

            final ConsumerRecords<Long, Object> records =
                    consumer.poll(Duration.ofSeconds(2));

            if (records.count() == 0) {
                System.out.println("None found");
            } else records.forEach(record -> {
            	Object obj = record.value();
            	
            	if(obj instanceof Employee) {
            		Employee employeeRecord = ((Employee) obj);
            		System.out.println("employee reocrd ============");
                    System.out.printf("%s %d %d %s \n", record.topic(),
                            record.partition(), record.offset(), employeeRecord);
                
            	}else {
            		Record value = (Record) record.value();
            		System.out.println("firstname : " + ((Utf8)value.get("firstName")).toString());
            		System.out.println("lastname : " + value.get("lastName"));

            		GenericRecord employeeRecord = ((GenericRecord) obj);
            		System.out.println("generic reocrd ============");
                    System.out.printf("%s %d %d %s \n", record.topic(),
                            record.partition(), record.offset(), employeeRecord);
            	}


            });
        });
    }


}
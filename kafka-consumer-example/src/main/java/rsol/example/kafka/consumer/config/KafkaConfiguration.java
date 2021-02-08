package rsol.example.kafka.consumer.config;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.SeekToCurrentBatchErrorHandler;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;
import org.springframework.util.StringUtils;

import rsol.example.kafka.consumer.config.JsonDeserializerWrapper;
import rsol.example.kafka.consumer.model.EventModel;

import lombok.extern.log4j.Log4j2;

@EnableKafka
@Configuration
@Log4j2
public class KafkaConfiguration {
	
	@Value("${spring.kafka.consumer.concurrency: 1}")
	private int CONURRENCY;
	
    @Bean
    public ConsumerFactory<String, CDCEventModel> consumerFactory(KafkaProperties properties) {
        Map<String, Object> config = properties.buildConsumerProperties();
//        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
//        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
      
        return new DefaultKafkaConsumerFactory<>(config, new StringDeserializer(),
                new JsonDeserializerWrapper<>(EventModel.class));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, EventModel> kafkaListenerContainerFactory(KafkaProperties properties) {
        ConcurrentKafkaListenerContainerFactory<String, EventModel> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory(properties));
        factory.setBatchListener(true);
        factory.setConcurrency(CONURRENCY);
        factory.setBatchErrorHandler(new SeekToCurrentBatchErrorHandler());
        factory.setRecordFilterStrategy(new RecordFilterStrategy<String, EventModel>() {

            @Override
            public boolean filter(ConsumerRecord<String, EventModel> consumerRecord) {
            		EventModel event = consumerRecord.value();

            		if(event == null || StringUtils.isEmpty(event.getTable()) || StringUtils.isEmpty(event.getOp_type())) {
            			log.warn("skipping record : " + event);
            			return true;
            		}else { return false; }
            }   
        });
        factory.setAckDiscarded(true);
        return factory;
    }

}

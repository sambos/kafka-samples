package rsol.example.kafka.consumer.listener;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import lombok.extern.log4j.Log4j2;
import rsol.example.kafka.consumer.constants.IConstants;
import rsol.example.kafka.consumer.service.KafkaConsumerService;

@Component
@Log4j2
public class KafkaConsumerListener {

	@Autowired
	private KafkaConsumerService service;

	@KafkaListener(topics = "sample.producer.avro.topic", groupId = "consumer-6", clientIdPrefix="str0")
	public void processMessage(String message) {
		
		try {
			System.out.println("received message :");
			System.out.println(message);
			if (message != null && message.trim().length() > 0) {
				service.process(null);
			}
		} catch (Exception e) {
			log.info(IConstants.EXCEPTION_OCCURED_WHILE_PROCESSING + e.getMessage(), e);
		}
	}
	
}

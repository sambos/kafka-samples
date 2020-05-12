package rsol.example.kafka.consumer.service;

import rsol.example.kafka.consumer.model.UserConfigModel;

public interface KafkaConsumerService {

	public UserConfigModel process(UserConfigModel model) throws Exception;

}

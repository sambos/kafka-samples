package rsol.example.kafka.producer.service;

import rsol.example.kafka.producer.model.UserConfigModel;

public interface ProducerService {

	public UserConfigModel sendMessage(UserConfigModel model) throws Exception;

}

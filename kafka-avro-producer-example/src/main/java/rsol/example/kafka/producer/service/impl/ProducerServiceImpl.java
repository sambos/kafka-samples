package rsol.example.kafka.producer.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.log4j.Log4j2;
import rsol.example.kafka.producer.constants.IConstants;
import rsol.example.kafka.producer.model.UserConfigModel;
import rsol.example.kafka.producer.service.ProducerService;
import rsol.example.kafka.producer.util.CommonUtil;

@Service
@Log4j2
public class ProducerServiceImpl implements ProducerService {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	private ObjectMapper mapper;
	
	@Value("${kafka.topic.name}") 
	private String topic;
	
	@Override
	public UserConfigModel sendMessage(UserConfigModel model) throws Exception {
		try {
			String message = toJson(prepareObject(model));
			log.info("topic: " + topic);
			log.info("sending message: " + message);
			ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, message);
			
		    future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
		    	 
		        @Override
		        public void onSuccess(SendResult<String, String> result) {
		            System.out.println("Sent message=[" + message + 
		              "] with offset=[" + result.getRecordMetadata().offset() + "]");
		        }
		        @Override
		        public void onFailure(Throwable ex) {
		            System.out.println("Unable to send message=["
		              + message + "] due to : " + ex.getMessage());
		        }
		    });
		    
		    
		} catch (Exception e) {
			log.info(IConstants.EXCEPTION_OCCURED_WHILE_PROCESSING, e.getMessage());
			e.printStackTrace();
		}
		return model;
	}

	private String toJson(Object obj) {
		try {
			return mapper.writeValueAsString(obj);
		}catch(Exception e) {
			return null;
		}
	}
	
	/**
	 * Method used for preparing the Object!.
	 * @param model
	 * @return
	 */
	private UserConfigModel prepareObject(UserConfigModel model)
	{
		if(!StringUtils.isEmpty(model.getUserId()))
		{
			model.setUpdatedDate(CommonUtil.getCurrentDateInString());
		}else{
			model.setCreatedDate(CommonUtil.getCurrentDateInString());
		}
		return model;
	}

}

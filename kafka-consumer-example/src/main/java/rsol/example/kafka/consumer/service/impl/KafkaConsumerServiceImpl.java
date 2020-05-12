package rsol.example.kafka.consumer.service.impl;

import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import lombok.extern.log4j.Log4j2;
import rsol.example.kafka.consumer.constants.IConstants;
import rsol.example.kafka.consumer.model.UserConfigModel;
import rsol.example.kafka.consumer.service.KafkaConsumerService;
import rsol.example.kafka.consumer.util.CommonUtil;

@Service
@Log4j2
public class KafkaConsumerServiceImpl implements KafkaConsumerService {

	/**
	 * Method used for Saving the Service Configuration Document!.
	 */

	@Override
	public UserConfigModel process(UserConfigModel model) throws Exception {
		try {
			//model = repository.save(prepareObject(model));
		} catch (Exception e) {
			log.info(IConstants.EXCEPTION_OCCURED_WHILE_PROCESSING, e.getMessage());
		}
		return model;
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

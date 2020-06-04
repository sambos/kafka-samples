package rsol.example.kafka.consumer.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.log4j.Log4j2;
import rsol.example.kafka.consumer.model.UserConfigModel;
import rsol.example.kafka.consumer.service.KafkaConsumerService;

@RestController
@Log4j2
@RequestMapping("/api-service/consumer/v1")
@Api(value = "consumer example ")
@CrossOrigin
public class KafkaConsumerController {

	@Autowired
	private KafkaConsumerService service;


	/**
	 * consume
	 * 
	 * @param user
	 * @return
	 */
	@PostMapping(value = "/consume", produces = "application/json")
	@ApiOperation(httpMethod = "POST", value = "Trigger manual consumer ", produces = "application/json", response = UserConfigModel.class)
	public ResponseEntity<?> saveReasonCode(@RequestBody(required = true) UserConfigModel model) {
		ResponseEntity<?> response = null;
		try {
			response = new ResponseEntity<>(service.process(model), HttpStatus.OK);

		} catch (Exception e) {
			log.info("Error occurred while processing!", e.getMessage());
			response = new ResponseEntity<>("Error occurred while processing!", HttpStatus.BAD_REQUEST);
		}
		return response;
	}

}

package rsol.example.kafka.producer.controller;

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
import rsol.example.kafka.producer.model.UserConfigModel;
import rsol.example.kafka.producer.service.ProducerService;

@RestController
@Log4j2
@RequestMapping("/api-service/producer/v1")
@Api(value = "Producer Service ! ")
@CrossOrigin
public class KafkaProducerController {

	@Autowired
	private ProducerService service;

	/**
	 * Publish message
	 * 
	 * @param user
	 * @return
	 */
	@PostMapping(value = "/publish", produces = "application/json")
	@ApiOperation(httpMethod = "POST", value = "publishes message to kafka topic", produces = "application/json", response = UserConfigModel.class)
	public ResponseEntity<?> send(@RequestBody(required = true) UserConfigModel model) {
		ResponseEntity<?> response = null;
		try {
			response = new ResponseEntity<>(service.sendMessage(model), HttpStatus.OK);

		} catch (Exception e) {
			log.info("Error occurred while processing!", e.getMessage());
			response = new ResponseEntity<>("Error occurred while processing!", HttpStatus.BAD_REQUEST);
		}
		return response;
	}

}

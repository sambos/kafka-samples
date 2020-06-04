package rsol.example.streamkafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.stereotype.Service;

import rsol.example.streamkafka.schema.Employee;

@Service
public class AvroConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroConsumer.class);

    @StreamListener(Processor.INPUT)
    public void consumeEmployeeDetails(Employee employeeDetails,
    		@Headers MessageHeaders messageHeaders) {
        LOGGER.info("Let's process employee details: {}", employeeDetails);
        
        LOGGER.info("- - - - - - - - - - - - - - -");
        messageHeaders.keySet().forEach(key -> {
            Object value = messageHeaders.get(key);
            if (key.equals("X-Custom-Header")){
            	LOGGER.info("{}: {}", key, new String((byte[])value));
            } else {
            	LOGGER.info("{}: {}", key, value);
            }
        });
    }

}
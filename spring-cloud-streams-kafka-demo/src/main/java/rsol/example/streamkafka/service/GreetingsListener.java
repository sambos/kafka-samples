package rsol.example.streamkafka.service;

import lombok.extern.slf4j.Slf4j;
import rsol.example.streamkafka.model.Greetings;
import rsol.example.streamkafka.stream.GreetingsStreams;

import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class GreetingsListener {
    @StreamListener(target = GreetingsStreams.OUTPUT)
    public void handleGreetings(@Payload Greetings greetings) {
        log.info("Received greetings: {}", greetings);
    }
}

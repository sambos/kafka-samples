package rsol.example.streamkafka.config;

import org.springframework.cloud.stream.annotation.EnableBinding;

import rsol.example.streamkafka.stream.GreetingsStreams;

@EnableBinding(GreetingsStreams.class)
public class StreamsConfig {
}

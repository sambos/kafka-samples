package rsol.example.streamkafka.producer;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import lombok.extern.log4j.Log4j2;

@Log4j2
public class MyProducerInterceptor implements ProducerInterceptor {

    private int onSendCount;
    private int onAckCount;

    @Override
    public ProducerRecord onSend(final ProducerRecord record) {
        onSendCount++;
        log.info(String.format("onSend topic=%s key=%s value=%s %d \n",
                    record.topic(), record.key(), record.value().toString(),
                    record.partition()
            ));
        return record;
    }

    @Override
    public void onAcknowledgement(final RecordMetadata metadata,
                                  final Exception exception) {
        onAckCount++;

        log.info(String.format("onAck topic=%s, part=%d, offset=%d, keySize=%d, valueSize=%d\n",
                    metadata.topic(), metadata.partition(), metadata.offset(), metadata.serializedKeySize(), metadata.serializedValueSize()
            ));
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
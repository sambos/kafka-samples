package rsol.example.kafka.consumer.util;

import lombok.Getter;
import lombok.Setter;

public class ConsumerOffsetDetails {

	@Getter
	@Setter
    private String topic;
	
	@Getter
	@Setter
    private Integer partition;
	
	@Getter
	@Setter
    private String group;
	
	@Getter
	@Setter
    private Short version;
	
	@Getter
	@Setter
    private Long offset;
	
	@Getter
	@Setter
    private String metadata;
	
	@Getter
	@Setter
    private Long commitTimestamp;
	
	@Getter
	@Setter
    private Long expireTimestamp;
    
}

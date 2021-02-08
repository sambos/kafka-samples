package rsol.example.kafka.consumer.model;

import java.util.Map;

import lombok.Getter;
import lombok.Setter;

public class EventModel{
	
	@Getter
	@Setter
    private String table;
	
	@Getter
	@Setter
    private String op_type;
	
	@Getter
	@Setter
	private String op_ts;

	@Getter
	@Setter
	private String current_ts;
    
	@Getter
	@Setter
	private String pos;
	
	@Getter
	@Setter
    private Map<String,Object> after;
	
	@Getter
	@Setter
    private Map<String,Object> before;
}

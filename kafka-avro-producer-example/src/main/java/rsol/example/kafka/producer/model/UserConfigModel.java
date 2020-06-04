package rsol.example.kafka.producer.model;

import java.io.Serializable;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Component
@JsonInclude(value = Include.NON_NULL)
@ToString
@NoArgsConstructor
public class UserConfigModel implements Serializable {
	/**
	* 
	*/
	private static final long serialVersionUID = 1L;

	@Getter
	@Setter
	private String userId;
	@Getter
	@Setter
	private String code;
	@Getter
	@Setter
	private String name;
	@Getter
	@Setter
	private String buId;
	@Getter
	@Setter
	private String createdBy;
	@Getter
	@Setter
	private String createdDate;
	@Getter
	@Setter
	private String updatedDate;
	@Getter
	@Setter
	private String updatedBy;
	@Getter
	@Setter
	private Integer status;
}

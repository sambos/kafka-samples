package rsol.example.kafka.producer.util;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
//import util.properties packages
import java.util.Properties;

//import simple producer packages
import org.apache.kafka.clients.producer.Producer;

//import KafkaProducer packages
import org.apache.kafka.clients.producer.KafkaProducer;

//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.util.ResourceUtils;

import com.fasterxml.jackson.databind.ObjectMapper;

public class KafkaSimpleProducerClient {
 
    //Assign topicName to string variable
    public static String topicName = "topic-name.json";
    
 public static void main(String[] args) throws Exception{

		send(read("file.json"));
	
 }
 
	public static void send(String msg, String key) {
		Producer<String, String> producer = getProducer();
		System.out.println("sending message : " + msg);
		producer.send(new ProducerRecord<String, String>(topicName, key, msg));
		producer.close();
	}
 
	public static void send(CDCEventModel event, String key) throws Exception {
		String msg = new ObjectMapper().writeValueAsString(event);
		send(msg, key);
	}
 

 public static Producer<String, String> getProducer() {
	    
	    // create instance for properties to access producer configs   
	    Properties props = new Properties();
	    
	    props.put("bootstrap.servers", "localhost:29092");	    
	    props.put("acks", "all");	    
	    props.put("retries", 0);	    
	    props.put("batch.size", 16384);	    
	    props.put("linger.ms", 1);	    
	    props.put("buffer.memory", 33554432);
	    
	    props.put("key.serializer", 
	       "org.apache.kafka.common.serialization.StringSerializer");
	       
	    props.put("value.serializer", 
	       "org.apache.kafka.common.serialization.StringSerializer");
	    
//	    props.put("value.serializer", 
//	    	       "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer");
	//    
	    Producer<String, String> producer = new KafkaProducer
	       <String, String>(props);
	    
	    return producer;
 }


 
 
	public static List<CDCEventModel> read(String name) throws Exception{
		String path = "classpath:" + name;
	    List<CDCEventModel> events = new ArrayList<>();
	    
	    ObjectMapper mapper = new ObjectMapper();
	    try {
	    	File file = ResourceUtils.getFile(path);
	    	List<String> lines = Files.readAllLines(file.toPath());
	    	for(String line : lines) {
	    		events.add(mapper.readValue(line, CDCEventModel.class));
	    	}

	    } catch (Exception e) {
	    	e.printStackTrace();
	    } 
	    return events;
	}
}

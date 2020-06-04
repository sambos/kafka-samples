package rsol.example.streamkafka.producer.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;

public class SchemaValidationTest {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub`
		Path p = Paths.get("src/main/resources/employee-schema.avsc");
		StringBuffer buffer = new StringBuffer();
		Files.lines(p).forEach(s -> buffer.append(s));
		
		
		MediaType SCHEMA_CONTENT =
	            MediaType.parse("application/vnd.schemaregistry.v1+json");
		
		String EMPLOYEE_SCHEMA = "{\"schema\":" + "\"" + buffer.toString().replaceAll("[\\t\\s]", "").replace("\"","\\\"" ) + "\"}";
		
		System.out.println(EMPLOYEE_SCHEMA);
		
		final OkHttpClient client = new OkHttpClient();
		
        //POST A NEW SCHEMA
        Request request = new Request.Builder()
                .post(RequestBody.create(SCHEMA_CONTENT, EMPLOYEE_SCHEMA))
                .url("http://localhost:8081/subjects/Employee/versions")
                .build();

        String output = client.newCall(request).execute().body().string();
        System.out.println(output);	
        
        
        //LIST ALL SCHEMAS
        request = new Request.Builder()
                .url("http://localhost:8081/subjects")
                .build();

        output = client.newCall(request).execute().body().string();
        System.out.println(output);
        
        
        //SHOW ALL VERSIONS OF EMPLOYEE
        request = new Request.Builder()
                .url("http://localhost:8081/subjects/Employee/versions/")
                .build();

        output = client.newCall(request).execute().body().string();
        System.out.println(output);      
        

        //SHOW VERSION 2 OF EMPLOYEE
        request = new Request.Builder()
                .url("http://localhost:8081/subjects/Employee/versions/2")
                .build();

        output = client.newCall(request).execute().body().string();
        System.out.println(output);
        
        
        //SHOW THE SCHEMA WITH ID 3
        request = new Request.Builder()
                .url("http://localhost:8081/schemas/ids/3")
                .build();

        output = client.newCall(request).execute().body().string();
        System.out.println(output);
        
        
        //SHOW THE LATEST VERSION OF EMPLOYEE 2
        request = new Request.Builder()
                .url("http://localhost:8081/subjects/Employee/versions/latest")
                .build();

        output = client.newCall(request).execute().body().string();
        System.out.println(output);
        
        
        
        //CHECK IF SCHEMA IS REGISTERED
        request = new Request.Builder()
                .post(RequestBody.create(SCHEMA_CONTENT, EMPLOYEE_SCHEMA))
                .url("http://localhost:8081/subjects/Employee")
                .build();

        output = client.newCall(request).execute().body().string();
        System.out.println(output);
        
        
        //TEST COMPATIBILITY
        request = new Request.Builder()
                .post(RequestBody.create(SCHEMA_CONTENT, EMPLOYEE_SCHEMA))
                .url("http://localhost:8081/compatibility/subjects/Employee/versions/latest")
                .build();

        output = client.newCall(request).execute().body().string();
        System.out.println(output);

        // TOP LEVEL CONFIG
        request = new Request.Builder()
                .url("http://localhost:8081/config")
                .build();

        output = client.newCall(request).execute().body().string();
        System.out.println(output);
	}

}

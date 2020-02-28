package com.github.ssomani7.elasticsearch_consumer;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParser;

public class ElasticSearchConsumer {
	//ES = ElasticSearch
	//Always keep the zookeeper and kafka server running before running any producer or client program.
	
	private static JsonParser jsonParser = new JsonParser(); //for extracting values from Json
	
	public static RestHighLevelClient createClient() { //creates ES client
		
		//Replcae with your own Bonsai ES Cluster Credentials. Don't include ':443' in the hostname.
		String hostname = "enter your own hostname from the account you create on bonsai.io";
		String username = "your own user name from the account created";
		String password = "your own password from the account you created";
		
		//don't do if you run a local ES. CredentialsProvider is needed for Bonsai Cloud Security purposes.
		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY, 
				new UsernamePasswordCredentials(username, password));
		
		RestClientBuilder builder = RestClient.builder(
				new HttpHost(hostname, 443, "https"))
				.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
					
					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
						return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
					}
				});
		
		RestHighLevelClient client = new RestHighLevelClient(builder);
		return client;
	}//end RestHighLevelClient
	
	public static KafkaConsumer<String, String> createConsumer(String topic){//topic in kafka we want to 
		//read from. In this case it will be 'twitter-tweets', but we can change it to whatever we want.
		//Code for this method taken from 'ConsumerDemoGroups.java' under the maven module 'kafka-basics'
		//**Note** = Kafka Consumer follow a "POLL" model which is why they need to ask the data from
		//Kafka system. Other messaging bus in enterprises follow "PUSH" model which is why data is provided
		//to them by servers automatically.
		
		String bootstrapServers = "localhost:9092";
		String groupId = "kafka-demo-elasticsearch";
		
		//Create conusmer configs. Our Consumer is by default 'At Least Once', because we didn't specify
		//the offset commit property explicitly and hence it is auto-commit offset.
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		// To disable auto commit of offsets we set the below property
		properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		// To decrease the amount of data being received use the below property
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
		
		//Create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		//subscribe the consumer to the topic
		consumer.subscribe(Arrays.asList(topic));
		
		return consumer;
	}//end createConsumer
	
	private static String extractIdFromTweet(String tweetJson) {
		// do this task by using the 'gson library'. Import it. GSON = JSON library by Google
		//We will need a JsonParser for this, so create one.
		return jsonParser.parse(tweetJson)
						 .getAsJsonObject()
						 .get("id_str") // 'id_str' is the field we want to extract.
						 .getAsString();
	}//end extractIdFromTweet
	
	public static void main(String[] args) throws IOException {
		//Create a logger
		Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
		RestHighLevelClient client = createClient();
				
		//create Kakfa Consumer
		KafkaConsumer<String, String> consumer = createConsumer("twitter-tweets");
		
		//Poll for new data. Consumer will not receive data until it asks for it explicitly.
		while(true) { //do not use this type of while loop in production.
			ConsumerRecords<String, String> records =
					consumer.poll(Duration.ofMillis(100)); //new in Kafka 2.0.0
			
			Integer recordCount = records.count();
			logger.info("Received " + recordCount + " records");
			
			//Performance improvement using batching. Creating a BulkRequest for multiple requests
			//process at once.
			BulkRequest bulkRequest = new BulkRequest();
			
			for(ConsumerRecord<String, String> record : records) {
				// Two strategies to create ID for making Consumer IDEMPOTENT.
				// 1. Kafka generic ID
				// String id = record.topic() + "_" + record.partition() + "_" + record.offset();
				
				// 2. Twitter feed specific ID. If we observe the ES console output from last run,
				// we can see a 'id_str' key in the JSON output and we need to figure out a way to
				// extract the value.
				try {
					String id = extractIdFromTweet(record.value()); // our own method to read data from JSON
					
					//We insert data into ElasticSearch here.
					@SuppressWarnings("deprecation")
					IndexRequest indexRequest = new IndexRequest(
							"twitter",
							"tweets",
							id //to make our consumer IDEMPOTENT 
							).source(record.value(), XContentType.JSON);
					
					bulkRequest.add(indexRequest); //we add to our bulk request (takes no time)
				} catch (NullPointerException e) { //added this particular exception after trial & error
					//method. Ran the code and came across this exception, so placed try-catch.
					logger.warn("skipping bad data: " + record.value());
				}
			}//end for loop
			
			if(recordCount > 0) {
				BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
				
				logger.info("committing offsets...");
				consumer.commitSync(); //To mannualy commit our offsets
				logger.info("Offsets have been committed");
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}//end if			
		}//end while loop
		
		//We have successfully managed to insert data from Kafka into ElasticSearch cluster in bonsai.io using
		//Kafka Consumer. Randomly used one of the 'id' generated in the Eclipse console after running 'ElasticSearchConsumer.java'
		//to test the ouput. Appended this 'id' to 'GET /twitter/tweets/id_generated_from_eclipse' to visualize
		//the tweet and its metadata in ElasticSearch Console.
		
		//close the client gracefully
		//client.close();
	}//end main

}//end class


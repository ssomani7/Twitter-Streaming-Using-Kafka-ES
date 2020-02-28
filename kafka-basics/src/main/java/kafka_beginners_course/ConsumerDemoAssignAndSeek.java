package kafka_beginners_course;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoAssignAndSeek {
	//Assign and Seek is another way of writing an application. It is less preferred way of writing
	//as compared to Group ID and subscribing to topic. We are just getting hands on for this approach as well.

	public static void main(String[] args) {
		//Testing to see if the project is setup correctly.
		System.out.println("hello world!");
		
		//Setup a logger
		Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());
		
		String bootstrapServers = "localhost:9092";
		String topic = "new_topic";
		
		//Create conusmer configs
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		//Create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		
		//assign and seek are mostly used to replay data or fetch a specific message
		
		//assign
		TopicPartition partitionToReadfrom = new TopicPartition(topic, 0);
		long offsetToReadFrom = 15L;
		consumer.assign(Arrays.asList(partitionToReadfrom));
		
		//seek
		consumer.seek(partitionToReadfrom, offsetToReadFrom);
		
		int numberOfMessagesToRead = 5;
		boolean keepOnReading = true;
		int numberOfMessagesReadSoFar = 0;
		
		//Poll for new data. Consumer will not receive data until it asks for it explicitly.
		while(keepOnReading) { //do not use this type of while loop in production.
			ConsumerRecords<String, String> records =
					consumer.poll(Duration.ofMillis(100)); //new in Kafka 2.0.0
			
			for(ConsumerRecord<String, String> record : records) {
				numberOfMessagesReadSoFar += 1;
				logger.info("Key: " + record.key() + ", Value: " + record.value());
				logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
				if(numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
					keepOnReading = false; //to exit the while loop
					break; //to exit the for loop
				}
			}
		}//end while
		
		logger.info("Exiting the application");
	}//end main

}//end class

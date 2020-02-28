package kafka_beginners_course;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoGroups {
//	Run this class multiple times without stopping the previous runs. Then run the 'ProducerDemoKeys.java'
//	Now observe the console output of the multiple running instances of 'ConsumerDemoGroups.java'
//	You can see the explicit load balancing between the instances specifying which instance is dealing
//	with what partition.

	public static void main(String[] args) {
//		Testing to see if the project is setup correctly.
		System.out.println("hello world!");
		
//		Setup a logger
		Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());
		
		String bootstrapServers = "localhost:9092";
		String groupId = "my-fifth-application";
		String topic = "new_topic";
		
//		Create conusmer configs
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
//		Create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		
//		Subscribe consumer to our topic(s)
		consumer.subscribe(Arrays.asList(topic));
		
//		Poll for new data. Consumer will not receive data until it asks for it explicitly.
		while(true) { //do not use this type of while loop in production.
			ConsumerRecords<String, String> records =
					consumer.poll(Duration.ofMillis(100)); //new in Kafka 2.0.0
			
			for(ConsumerRecord<String, String> record : records) {
				logger.info("Key: " + record.key() + ", Value: " + record.value());
				logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
			}
		}//end while
	}//end main

}//end class

package kafka_beginners_course;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo {

	public static void main(String[] args) {
//		testing to see if the project has been setup correctly
		System.out.println("hello world!");
		
//		3 steps for starting a producer
//		Step 1 = create Producer properties
//		Step 2 = create the Producer
//		Step 3 = send data
		
		String bootstrapServers = "127.0.0.1:9092";
//		Step 1 = Creating Producer Properties. Refer to Official Kafka documentation. See Producer Configs for details.
		Properties properties = new Properties();
//		'bootstrap.servers' is a required property as per documentation. Create a variable for localhost address 
//		and kafka port number. Place the address variable at the top.
//		***Below 2 lines are the old way of declaring 'bootstrap.servers' property.***
//		properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
//		properties.setProperty("bootstrap.servers", bootstrapServers);
//		***Use the below syntax as a modern standard for declaring properties***
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//		2nd thing we have to create is key and value serializer. 
//		***Note*** = These 2 serializers help the producer know what type of values you are sending to kafka. This 
//		will help understand the kafka client to convert the values to bytes (0's & 1's)
//		***Old way of declaring 'key.serializer' property.***
//		properties.setProperty("key.serializer", StringSerializer.class.getName());
//		***Use the below syntax as a modern standard for declaring properties***
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//		***Old way of declaring 'key.serializer' property.***
//		properties.setProperty("value.serializer", StringSerializer.class.getName());
//		***Use the below syntax as a modern standard for declaring properties***
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
//		Step 2 = Create a Producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
//		Step 3 = Send data. Send() needs a producer record, so create that first.
//		Creating a producer record.
		ProducerRecord<String, String> record = 
				new ProducerRecord<String, String>("new_topic", "hello world!");
//		***Note*** send data method - this is Asynchronous in nature. So sending data works in background.
		producer.send(record);
//		***Note*** So in order to data being sent first and then allowing the program to terminate, use the flush
//		functionality, which causes all data to be sent. Then close the producer.
//		flush data
		producer.flush();
//		flush and close the producer
		producer.close();
		
//		To understand where the message was produced, whether it was produced correctly and to understand the offset
//		value and the partition values. This is achieved using 'Callback'. See the class 'ProducerDemoWithCallback'
//		for this.
	}//end main

}//end class

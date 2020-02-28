package kafka_beginners_course;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoWithThreads {
	//The new thread inner class is implemented below. Purpose of it to replace the 'while(true)' part 
	//with standard thread practices to run multiple instances of a class.

	public static void main(String[] args) {
		new ConsumerDemoWithThreads().run();
	}//end main
	
	private ConsumerDemoWithThreads(){
		//empty constructor
	}//end constructor
	
	private void run() {
		//Setup a logger
		Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class.getName());
		
		String bootstrapServers = "localhost:9092";
		String groupId = "my-sixth-application";
		String topic = "new_topic";
		
		//Latch for dealing with multiple threads
		CountDownLatch latch = new CountDownLatch(1);
		logger.info("Creating the consumer thread");
		
		//Create the consumer runnable
		Runnable myConsumerRunnable = new ConsumerRunnable(bootstrapServers,
														groupId,
														topic,
														latch);
		
		//Start the thread
		Thread myThread = new Thread(myConsumerRunnable);
		myThread.start();
		
		//Add a shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread( () -> {
			logger.info("caught shutdown hook");
			((ConsumerRunnable) myConsumerRunnable).shutdown();
			try {
				latch.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			logger.info("Application has exited");
		}
		));
		
		//we don't want our application to exit right away.
		try {
			latch.await();
		} catch (InterruptedException e) {
			logger.error("Application got interrupted", e);
		} finally {
			logger.info("Application is closing");
		}
	}//end run
	
	//***Thread Class from below for KafkaConsumer***
	public class ConsumerRunnable implements Runnable{

		//Constructor for consumer thread which is going to have a parameter called 'latch'
		//Latch is used in java to deal with concurrency. And this Latch will help us shut down
		//the threads properly.
		private CountDownLatch latch;
		private KafkaConsumer<String, String> consumer;
		
		//Setup a logger
		private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());
		
		public ConsumerRunnable(String bootstrapServers,
							  String groupId,
							  String topic,
							  CountDownLatch latch) { //constructor
			this.latch = latch;
			
			//Create conusmer configs
			Properties properties = new Properties();
			properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
			properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			
			//Create consumer
			consumer = new KafkaConsumer<String, String>(properties);
			//Subscribe consumer to our topic(s)
			consumer.subscribe(Arrays.asList(topic));
		}//end constructor
		
		public void run() {
			try {
				//Poll for new data. Consumer will not receive data until it asks for it explicitly.
				while(true) { //do not use this type of while loop in production.
					ConsumerRecords<String, String> records =
							consumer.poll(Duration.ofMillis(100)); //new in Kafka 2.0.0
					
					for(ConsumerRecord<String, String> record : records) {
						logger.info("Key: " + record.key() + ", Value: " + record.value());
						logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
					}
				}//end while
			} catch (WakeupException e) {
				logger.info("Received shutdown signal!");
			} finally {
				//***Note*** = super important to close our consumer once we are done.
				consumer.close();
				//Tell our main code we're done with the consumer.
				latch.countDown();
			}		
		}//end run
		
		public void shutdown() { //will shutdown the consumer threads.
			//the wakeup() method is a special method to interrupt consumer.poll()
			//It will throw the exception WakeupException
			consumer.wakeup();		
		}//end shutdown
		
	}//end class ConsumerThread

}//end class ConsumerDemoWithThreads

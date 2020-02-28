package kafka_twitter_project;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterProducer {
	// Setup a logger.
	Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
	
	// All these are available when you create a Twitter Developer Account. Input your own credentials.
	String consumerKey = "enter consumerKey from the credentials of your own twitter account"; // API Key
	String consumerSecret = "enter consumerSecret from the credentials of your own twitter account"; // API Secret Key
	String token = "enter access token from the credentials of your own twitter account"; // Access Token
	String secret = "enter access token secret key from the credentials of your own twitter account"; // Access Token Secret Key
	
	// search term on twitter to be entered in parameter.
	List<String> terms = Lists.newArrayList("LFC", "Bryant"); //any topic you want.
	
	public TwitterProducer() {
		// empty constructor
	}

	public static void main(String[] args) {
		new TwitterProducer().run();
	}// end main

	public void run() {
		logger.info("Setup");
		
		/** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
		// Client will extract the messages and put them into the message queue.
		
		// create a twitter client
		Client client = createTwitterClient(msgQueue);
		//  Attempts to establish a connection.
		client.connect();
		
		// create a kafka producer
		KafkaProducer<String, String> producer = createKafkaProducer();
		
		// add a shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread( () -> {
			logger.info("stopping application...");
			logger.info("shutting down client from twitter...");
			client.stop();
			logger.info("closing producer...");
			producer.close();
			logger.info("done!");
		}));
		
		// loop to send tweets to kafka
		//  on a different thread, or multiple different threads....
		while (!client.isDone()) {
			String msg = null;
			try {
				msg = msgQueue.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
				client.stop();
			}
			if (msg != null) {
				logger.info(msg);
				// A topic must exist in Kafka for a producer to send data. So first create a topic.
				// In this case, we are first creating a topic named "twitter-tweets" in Kafka first and then
				// run this code.
				producer.send(new ProducerRecord<String, String>("twitter-tweets", null, msg), new Callback() {
					
					@Override
					public void onCompletion(RecordMetadata recordMetaData, Exception e) {
						if(e != null) {
							logger.error("Something bad happened", e);
						}
					}
				});
			}// end if
			logger.info("End of application");
		}
	}// end run
		
	public Client createTwitterClient(BlockingQueue msgQueue) {
		// Following code has been referred from "https:// github.com/twitter/hbc"
		
		/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		
		hosebirdEndpoint.trackTerms(terms);

		//  These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);
		
		// Creating a client
		ClientBuilder builder = new ClientBuilder()
				  .name("Hosebird-Client-01")                              //  optional: mainly for the logs
				  .hosts(hosebirdHosts)
				  .authentication(hosebirdAuth)
				  .endpoint(hosebirdEndpoint)
				  .processor(new StringDelimitedProcessor(msgQueue));

				Client hosebirdClient = builder.build();
				return hosebirdClient;		
	}// end createTwitterClient
	
	public KafkaProducer<String, String> createKafkaProducer(){		
		String bootstrapServers = "localhost:9092";
		
		// Creating Producer Properties. Refer to Official Kafka documentation. See Producer Configs for details.
		Properties properties = new Properties();
		// 'bootstrap.servers' is a required property as per documentation. Create a variable for localhost address 
		// and kafka port number. Place the address variable at the top.
		// ***Below 2 lines are the old way of declaring 'bootstrap.servers' property.***
		// properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
		// properties.setProperty("bootstrap.servers", bootstrapServers);
		// ***Use the below syntax as a modern standard for declaring properties***
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		// 2nd thing we have to create is key and value serializer. 
		// ***Note*** = These 2 serializers help the producer know what type of values you are sending to kafka. This 
		// will help understand the kafka client to convert the values to bytes (0's & 1's)
		// ***Old way of declaring 'key.serializer' property.***
		// properties.setProperty("key.serializer", StringSerializer.class.getName());
		// ***Use the below syntax as a modern standard for declaring properties***
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		// ***Old way of declaring 'key.serializer' property.***
		// properties.setProperty("value.serializer", StringSerializer.class.getName());
		// ***Use the below syntax as a modern standard for declaring properties***
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		// create a safe producer
		properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
		// Kafka 2.0 >= 1.1 so we can keep this as 5. Use 1 otherwise.
		properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
		
		// higher throughput producer (at the expense of a bit of latency and CPU usage)
		properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20"); // delay of 20 milli second
		properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); // 32 KB batch size
		
		// Create a Producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		return producer;
	}// end createKafkaProducer
}// end class

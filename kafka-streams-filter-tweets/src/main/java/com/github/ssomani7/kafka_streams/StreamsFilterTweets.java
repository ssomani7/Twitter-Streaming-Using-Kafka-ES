package com.github.ssomani7.kafka_streams;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.KStream;

import com.google.gson.JsonParser;

public class StreamsFilterTweets {

	private static JsonParser jsonParser = new JsonParser(); //for extracting values from Json
	
	private static Integer extractUserFollowersInTweet(String tweetJson) {
		// do this task by using the 'gson library'. Import it. GSON = JSON library by Google
		//We will need a JsonParser for this, so create one.
		try {
			return jsonParser.parse(tweetJson)
					 .getAsJsonObject()
					 .get("user") // 'user' is the field we want to extract from ES content.
					 .getAsJsonObject()
					 .get("followers_count")
					 .getAsInt();
		} catch (NullPointerException e) {
			return 0;
		}
	}//end extractIdFromTweet
	
	public static void main(String[] args) {
		//create properties
		Properties properties = new Properties();
		properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
		properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
		properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
		
		//create a topology
		StreamsBuilder streamsBuilder = new StreamsBuilder();
		
		// input topic
		KStream<String, String> inputTopic = streamsBuilder.stream("twitter-tweets");
		KStream<String, String> filteredStream = inputTopic.filter(
				//filter for tweets which has a user of over 10000 followers
				(k,jsonTweet) -> extractUserFollowersInTweet(jsonTweet) > 10000
		);
		filteredStream.to("important-tweets");
		
		//build the topology
		KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
		
		//start our streams application. IllegalStateException, StreamsException 
		try {
			kafkaStreams.start();
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (StreamsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}//end main

}//end class

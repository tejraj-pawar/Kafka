package com.yolo.kafka.streams;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.google.gson.JsonParser;

public class FilterTweetsUsingStream {
	/*
	 * This class will filter tweets based on user's followers
	 */
	
	public static Logger log = Logger.getLogger(FilterTweetsUsingStream.class);
	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j.properties");
		String bootStrapServer = "127.0.0.1:9092";
		String streamAppId = "demo-kafka-streams"; //its like consumer grp name.
		
		// create properties
		Properties properties = new Properties();
		properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG , bootStrapServer);
		properties.put(StreamsConfig.APPLICATION_ID_CONFIG , streamAppId);
		//properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG , Serdes.StringSerde.class.getName());
		//properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG , Serdes.StringSerde.class.getName());
		properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG , Serdes.String().getClass());
		properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG , Serdes.String().getClass());
				
		
		// create a stream topology
		StreamsBuilder streamsBuilder= new StreamsBuilder();
		KStream<String, String> streamOfTweets = streamsBuilder.stream("twitter_tweets"); // assign topic to stream.
		KStream<String, String> filteredStream = streamOfTweets.filter(
				//filter for tweets which has over 10000 followers.
				(key, jsonTweet) ->  extractUserFollowersFromTweet(jsonTweet) > 10000
				);
		filteredStream.to("important_tweets"); // send filtered tweet to this topic.
		
		// build a stream topology
		KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
		
		// start stream application
		kafkaStreams.start();
		System.out.println();
	}
	
	private static JsonParser jsonParser = new JsonParser();
	static int userFollowers = 0;
	private static Integer extractUserFollowersFromTweet(String tweetJson) {
		try {
			userFollowers = jsonParser.parse(tweetJson).getAsJsonObject().get("user")
				.getAsJsonObject().get("follower_count").getAsInt();
			log.info("User has " +userFollowers+ " followers");
			return userFollowers;
		}
		catch(NullPointerException e) {
			return 0;
		}
		}

}

package com.yolo.kafka.elasticsearch;

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
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import com.google.gson.JsonParser;

public class ElasticsearchIdempotentConsumer {
	/**
	 * NOTE: We have created elasticsearch cluster in cloud using Bonsai.
	 * here we're polling kafka and send those messages to elasticsearch in batches.
	 * here we are committing offsets manually after messages are processed(see consumer properties.)
	 */
	public static Logger log = Logger.getLogger(ElasticsearchIdempotentConsumer.class);

	public static void main(String[] args) throws IOException {
		PropertyConfigurator.configure("log4j.properties");

		// create elsticsearch client
		RestHighLevelClient client = createClient();
		System.out.println("Elasticsearch Client: " + client);

		// create kafka consumer
		KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");
		// poll for the data
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			int recordCount = records.count();
			log.info("Recieved " + recordCount + " records from kafka");
			
			// to stores records in batches and send records to ES in batches to improve performance.
			BulkRequest bulkRequest = new BulkRequest();
			
			for (ConsumerRecord<String, String> record : records) 
			{
				// first strategy to create unique Id using kafka components.
				//String id = record.topic() +"_"+ record.partition() +"_"+ record.offset();
				
				// second strategy to create unique Id from tweet recieved.
				String id = null;
				try {
					id = extractIdFromTweet(record.value());
				} catch (Exception e) {
					log.info("Skipping bad data: " + record.value());
					continue;
				}
				
				// here we will insert data into elasticsearch, send data to index named 'twitter'
   				// now here we'll pass id as well so that duplicate messages won't enter elasticsearch.(i.e. idempotent consumer) 
				IndexRequest indexRequest = new IndexRequest("twitter", "tweets", id).source(record.value(), XContentType.JSON);
				
				// add request to bulk request (takes no time)
				bulkRequest.add(indexRequest);
			}
			
			if(recordCount > 0)
			{
				// send data to ES in bulk i.e. in batches
				log.info("Sending " + recordCount + " tweets in this batch to ES");
				BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
				
				log.info("Commiting offsets...");
				consumer.commitSync();
				log.info("Offsets have been commited.");
				
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
		// close the client
		//client.close();
	}

	public static KafkaConsumer<String, String> createConsumer(String topic) {
		String bootstrapservers = "127.0.0.1:9092";
		String groupId = "kafka-demo-elasticsearch";
		// String topic = "twitter_tweets";

		// create consumer properties
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapservers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId); // optional
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // earliest/latest/none //optional
		properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // disable auto commit of offsets.
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "50"); // to poll only 10 records at a time.
		
		// create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		consumer.subscribe(Arrays.asList(topic));
		return consumer;
	}
	
	private static JsonParser jsonParser = new JsonParser();
	private static String extractIdFromTweet(String tweetJson) {
		return jsonParser.parse(tweetJson).getAsJsonObject().get("id_str").getAsString();
	}

	public static RestHighLevelClient createClient() {
		/**
		 * Below is the elasticsearch access url from bonsai console:
		 * https://sre3fu5gyy:znxl1m9aoh@kafka-elasticsearch--5150414490.eu-central-1.bonsaisearch.net:443
		 * https://username:password@hostname
		 */

		String hostname = "kafka-elasticsearch--5150414490.eu-central-1.bonsaisearch.net";
		String username = "sre3fu5gyy";
		String password = "znxl1m9aoh";

		// don't do this if you run a local Elasticsearch.
		final CredentialsProvider credentialProvider = new BasicCredentialsProvider();
		credentialProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

		RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"))
				.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
					@Override
					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
						return httpClientBuilder.setDefaultCredentialsProvider(credentialProvider);
					}
				});
		RestHighLevelClient client = new RestHighLevelClient(builder);
		return client;
	}

}

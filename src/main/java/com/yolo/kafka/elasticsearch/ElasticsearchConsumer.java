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
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

public class ElasticsearchConsumer {
	/**
	 * NOTE: We have created elasticsearch cluster in cloud using Bonsai.
	 * here we'll poll kafka and then send those messages to elasticsearch
	 */
	public static Logger log = Logger.getLogger(ElasticsearchConsumer.class);

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
			for (ConsumerRecord<String, String> record : records) 
			{
				/*
				 * log.info("=======================\n" + 
			             "Fetched new message: \n" + "Key: " + record.key() + "  " + "Message: " 
						+ record.value() + "\n" + "Topic: " + record.topic() + "\n" + "Partition: "
						+ record.partition() + "\n" + "Offset: " + record.offset() + "\n" + "TimeStamp: "
						+ record.timestamp());
				 */
				// here we will insert data into elasticsearch, send data to index named 'twitter'
				String jsonToSend = record.value();
				IndexRequest indexRequest = new IndexRequest("twitter", "tweets").source(jsonToSend, XContentType.JSON);
				IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
				String id = indexResponse.getId();
				log.info("Id of the msg sent: " + id);
				try {
					Thread.sleep(1000); //just to slowdown process for observation.
				} catch (InterruptedException e) {
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
		// deserializer properties to convert bytes to string while consuming messages from topic.

		// create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		consumer.subscribe(Arrays.asList(topic));
		return consumer;
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

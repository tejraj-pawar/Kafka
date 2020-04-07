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

public class ElasticsearchConsumerDemo {
	/**
	 * NOTE: We have created elasticsearch cluster in cloud using Bonsai.
	 * here we're creating elasticsearch client and then will send data to elasticsearch
	 */
	public static Logger log = Logger.getLogger(ElasticsearchConsumer.class);

	public static void main(String[] args) throws IOException {
		PropertyConfigurator.configure("log4j.properties");

		// create elsticsearch client
		RestHighLevelClient client = createClient();
		System.out.println("Elasticsearch Client: " + client);

		String jsonToSend = "{ \"tweet\":\"this is sample tweet\" }";

		// send data to index named 'twitter'
		IndexRequest indexRequest = new IndexRequest("twitter", "tweets").source(jsonToSend, XContentType.JSON);
		IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
		String id = indexResponse.getId();
		log.info("Id of the msg sent: " + id);

		// close the client
		client.close();

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

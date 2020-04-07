package com.yolo.kafka.twitter;

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
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

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
	static Logger log = Logger.getLogger(TwitterProducer.class);
	static int countMessages = 0;
	// Twitter developer credentials. Its not a good practice to include any credentials in code.
	String consumerKey = "*********oydQOKvLvXf6zoZ";
	String consumerSecret = "*********dEUWWwT6QYkP9oEJ4yBtqq5Po7Zyw3wNX6hNBT";
	String token = "*********09890-O6AWWD2Nn69cgiidKvWbzbMfSsQn1Q";
	String secret = "*********sUhjtpKJdsdmEpAC8XUb2GdWy";

	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j.properties");
		new TwitterProducer().run();
	}

	public void run() {
		/**
		 * Set up your blocking queues: Be sure to size these properly based on expected
		 * TPS of your stream
		 */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

		// create a twitter client
		Client client = createTwitterClient(msgQueue);
		client.connect(); // establish connection

		// create kafka producer
		KafkaProducer<String, String> producer = createKafkaProducer();
        
		// add shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread( () -> {
			log.info("Stopping application....");
			log.info("shutting down client from twitter...");
			client.stop();
			log.info("closing kafka producer...");
			producer.close();
		}));
		
		// create callback(A callback interface that the user can implement to allow
		// code to execute when the request is complete.)
		Callback callback = new Callback() {
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				// executes every time when record is successfully sent to topic or an exception
				// is thrown.
				if (exception == null) {
					// the record was successfully sent
					/*
					 * log.info("Received new metadata: \n" + "Topic: " + metadata.topic() + "\n" + "Partition: "
							+ metadata.partition() + "\n" + "Offset: " + metadata.offset() + "\n" + "TimeStamp: "
							+ metadata.timestamp());
					*/
				} else {
					// exception occurred
					log.error("Error while producing msg to kafka topic: ", exception);
				}
			}
		};

		// loop to send tweets to kafka
		while (!client.isDone()) {
			String msg = null;
			try {
				msg = msgQueue.poll(5, TimeUnit.SECONDS);
			} catch (Exception e) {
				e.printStackTrace();
				client.stop();
			}
			if (msg != null) {
				countMessages++;
				log.info("Count: " + countMessages +" Message: " + msg);
				producer.send(new ProducerRecord<String, String>("twitter_tweets", null, msg), callback);
			}
		}
		log.info("End of Application");
		client.stop();
	}

	public KafkaProducer<String, String> createKafkaProducer() {
		String bootStrapServers = "127.0.0.1:9092";
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // add below properties to create a safer producer
		properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
		properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); // for kafka 1.1, we can use it as 5 otherwise 1.		
		// high throughput producer (at the expense of a bit of latency(linger) and CPU usage(compression)))
		properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
		properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)); //32 kb batch size (default:16kb)
		
		// create the producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		return producer;
	}

	public Client createTwitterClient(BlockingQueue<String> msgQueue) {
		/**
		 * Declare the host you want to connect to, the endpoint, and authentication
		 * (basic auth or oauth)
		 */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		//this will add filter(it will tell client to fetch tweets related to these keywords only)
		List<String> termsToConsider = Lists.newArrayList("kafka"); 
		hosebirdEndpoint.trackTerms(termsToConsider);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

		ClientBuilder builder = new ClientBuilder().name("Hosebird-Client-01") // optional: mainly for the logs
				.hosts(hosebirdHosts).authentication(hosebirdAuth).endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));

		Client hosebirdClient = builder.build();
		return hosebirdClient;
	}

}

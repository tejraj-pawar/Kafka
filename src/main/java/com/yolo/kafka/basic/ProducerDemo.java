package com.yolo.kafka.basic;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo {

	public static void main(String[] args) {
		// create producer properties
		// for producer properties, refer https://kafka.apache.org/documentation/#producerconfigs
		String bootStrapServers = "127.0.0.1:9092";
		Properties properties = new Properties();
		/*
		 * properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
		 * properties.setProperty("key.serializer", StringSerializer.class.getName());
		 * properties.setProperty("value.serializer",
		 * StringSerializer.class.getName());
		 we can write above properties as:
		 */
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		//serializer properties to convert string to bytes while sending messages to topic.
		
		//create the producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
	
		//create producer record
		ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world");
	    
		//send data - asynchronous (bcoz send() wont send data to kafka topic, for that you have to flush data from producer as well)
		producer.send(record);
		//flush data from producer(i.e flush actually sends data to topic)
		producer.flush();
		//flush data and close producer
		producer.close();
		
		System.out.println("Message sent to kafka topic!!");
 
	}

}










package com.yolo.kafka.basic;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class ConsumerDemo {
	static Logger log = Logger.getLogger(ConsumerDemo.class);

	public static void main(String[] args) { 
		PropertyConfigurator.configure("log4j.properties");
		String bootstrapservers = "127.0.0.1:9092";
		String groupId = "kafka-comsumer-grp-1";
		String topic = "first_topic";
		
		//create consumer properties
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapservers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,  StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);  //optional
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // earliest/latest/none  //optional
		//deserializer properties to convert bytes to string while consuming messages from topic.
		
		//create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

		//subscribe consumer to one topic
		consumer.subscribe(Arrays.asList(topic));
		
		//subscribe consumer to multiple topic
		//consumer.subscribe(Arrays.asList("first_topic", "Second_topic", "3rd_topic"));

		//poll for the data
		while(true)
		{
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, String> record : records)
            {
            	log.info("=======================\n" + 
            			"Received new message: \n" +
            			"Key: " + record.key() + "  " + "Message: " + record.value() + "\n" +
			             "Topic: " + record.topic() + "\n" +
			            "Partition: " + record.partition() + "\n" +
			            "Offset: " + record.offset() + "\n" +
			            "TimeStamp: " + record.timestamp());
            }
            	
		}
		//consumer.close();
	}

}

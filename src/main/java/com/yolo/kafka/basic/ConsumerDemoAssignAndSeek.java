package com.yolo.kafka.basic;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class ConsumerDemoAssignAndSeek {
	static Logger log = Logger.getLogger(ConsumerDemo.class);

	public static void main(String[] args) { 
		PropertyConfigurator.configure("log4j.properties");
		String bootstrapservers = "127.0.0.1:9092";
		String topic = "first_topic";
		
		//create consumer properties
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapservers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,  StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // earliest/latest/none  //optional
        // Note here that, we have removed groupId property.
		
		//create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

		//subscribe consumer to one topic
		//consumer.subscribe(Arrays.asList(topic));
		
		// Assign and Seek are mostly  used to replay data or fetch specific message from specific topic-partition-offset.
		// Assign & Seek are the alternatives of subscribe().
		
		// Assign (i.e. assign consumer to specific topic-partition)
		TopicPartition partitionToReadFrom = new TopicPartition(topic, 2); //(topic, partitionId)
        consumer.assign(Arrays.asList(partitionToReadFrom));
        
        // Seek (i.e. fetch data from specific offset from above assigned partition )
		long offsetToBeReadFrom = 4L;
        consumer.seek(partitionToReadFrom, offsetToBeReadFrom);
        
		//poll for the data (this will poll data from partition offset=4)
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

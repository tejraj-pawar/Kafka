package com.yolo.kafka.basic;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
//import org.apache.logging.log4j.Logger;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;


public class ProducerDemoWithCallback {
	//static Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
	static Logger log = Logger.getLogger(ProducerDemoWithCallback.class);

	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j.properties");
		// create producer properties
		// for producer properties, refer https://kafka.apache.org/documentation/#producerconfigs
		String bootStrapServers = "127.0.0.1:9092";
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		
		//create the producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
	
		//create producer record
		ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world with callback1");
	    
		//create callback(A callback interface that the user can implement to allow code to execute when the request is complete.)
		Callback callback = new Callback() {
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				// executes every time when record is successfully sent to topic or an exception is thrown.
				if(exception == null)
				{
					// the record was successfully sent
					log.info("Received new metadata: \n" +
					            "Topic: " + metadata.topic() + "\n" +
					            "Partition: " + metadata.partition() + "\n" +
					            "Offset: " + metadata.offset() + "\n" +
					            "TimeStamp: " + metadata.timestamp());
				}
				else
				{
					// exception occurred
					log.error("Error while producing: ", exception);
 				}

			}
		};

		//send data - asynchronous (bcoz send() wont send data to kafka topic, for that you have to flush data from producer as well)
		producer.send(record, callback);
		
		//flush data from producer(i.e flush actually sends data to topic)
		producer.flush();
		
		//flush data and close producer
		producer.close();
		
		System.out.println("Message sent to kafka topic!!");
	}

}











package com.yolo.kafka.basic;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class ConsumerDemoWithThreads {
	static Logger log = Logger.getLogger(ConsumerDemo.class);

	public static void main(String[] args) { 
		PropertyConfigurator.configure("log4j.properties");
        new ConsumerDemoWithThreads().run();
	}
	
	private ConsumerDemoWithThreads() {
	}
	
	private void run() {
		String bootstrapservers = "127.0.0.1:9092";
		String groupId = "kafka-comsumer-grp-5";
		String topic = "first_topic";
		
		// latch for dealing with multiple threads
		CountDownLatch latch  = new CountDownLatch(1);
		
		// create the consumer runnable
		log.info("Creating the consumer thread");
		Runnable myConsumerRunnable = new ConsumerRunnable(bootstrapservers, groupId, topic, latch);
	    
		// start the thread
	    Thread myThread = new Thread(myConsumerRunnable);
	    myThread.start();
	    
	    // add shutdown hook
	    Runtime.getRuntime().addShutdownHook(new Thread( () -> {
	    	log.info("Caught shutdown hook");
	    	((ConsumerRunnable)myConsumerRunnable).shutdown();
	    	try {
				latch.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
	    	log.info("Application has exited");
	    }
	    ));
	    
	    try {
			latch.await();
		} catch (InterruptedException e) {
			log.error("Application got Interupted: ", e);
		} finally {
		    log.info("Application is closing");
		}
	    
	}
	
	public class ConsumerRunnable implements Runnable{
		private CountDownLatch latch;
		private KafkaConsumer<String, String> consumer;

        public ConsumerRunnable(String bootstrapservers,
        		String groupId, String topic, CountDownLatch latch) {
        	this.latch = latch;
        	//create consumer properties
    		Properties properties = new Properties();
    		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapservers);
    		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,  StringDeserializer.class.getName());
    		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);  
    		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        	
        	consumer = new KafkaConsumer<String, String>(properties);
        	consumer.subscribe(Arrays.asList(topic));
        }
        
		public void run() {
			//poll for the data
			try {
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
			} catch (WakeupException e) {
				log.error("Recieved shutdown Singnal!!");
			} finally {
				consumer.close();
				// tell our main code that we're done with consumer
				latch.countDown();
			}
		}
		
		// This will shutdown our consumer thread.
		public void shutdown() {
			// the wakeup() method is a special method to interrupt consumer.poll()
			// it will throw the wakeup exception.
			consumer.wakeup();
		}
		
	}

}

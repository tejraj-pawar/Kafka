## KAFKA

**This project is performing following task:**

*Real-Time Twitter Tweets -> Kafka Producer -> **KAFKA** -> Kafka Consumer -> ElasticSearch Cluster*

* #### Packages and files inside **src/main/java** and their description:

##### 1] com.yolo.kafka.basic: 
-It contains sample kafka producer and consumers.

##### 2] com.yolo.kafka.twitter.TwitterProducer.java: 
-This file polls tweets from twitter and push those tweets to kafka topic.
                             
##### 3] com.yolo.kafka.elasticsearch.ElasticsearchIdempotentConsumer.java:  
-here we're polling kafka and sending those messages to elasticsearch cluster in batches and we are committing offsets manually after messages are processed(see consumer properties.)

##### 4] com.yolo.kafka.streams.FilterTweetsUsingStream.java: 
-This files takes tweets from kafka and pushes only those tweets back to kafka having user's follower count > 1000.

* #### kafka-window-setup.txt: This file contains how to setup KAFKA and ZOOKEEPER.

* #### kafka-window-setup.txt: This file contains KAFKA CLI Commands for creation of topic, cosumer, producer, consumer-group, etc..

* #### /kafka-stack-docker-compose-master: This folder contains docker-compose files for Single/Mutli Zookeeper and Single/Multi Broker kafka cluster.

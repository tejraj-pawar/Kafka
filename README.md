## KAFKA

** This project is performing following task: **
* Real-Time Tiwtter Tweets -> Kafka Producer -> **KAFKA** -> Kafka Consume -> ElasticSearch Cluster *

1] com.yolo.kafka.basic 
-It contains sample kafka producer and consumers.

2] com.yolo.kafka.twitter
-TwitterProducer.java 
--This file polls tweets from twitter and push those tweets to kafka topic.
                             
3] com.yolo.kafka.elasticsearch
-ElasticsearchIdempotentConsumer.java  
--here we're polling kafka and sending those messages to elasticsearch cluster in batches.
--here we are committing offsets manually after messages are processed(see consumer properties.)

4] com.yolo.kafka.streams
-FilterTweetsUsingStream.java 
--This files takes tweets from kafka and pushes only those tweets back to kafka having user's follower count > 1000.

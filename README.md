# Data Engineer Challenge

Streaming data from [Kafka](http://kafka.apache.org/) and counting unique things within this data.

Details in [doc/data_engineer_challenge.doc](https://github.com/ninapetkovic/kafka-stream-count/blob/master/doc/data_engineer_challenge.pdf)

## Kafka installation  

     tar -xzf kafka_2.12-2.3.0.tgz

     cd kafka_2.12-2.3.0

     bin/zookeeper-server-start.sh config/zookeeper.properties

     bin/kafka-server-start.sh config/server.properties

## Kafka topic creation

     bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic tamedia-topic-source

     bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic tamedia-topic-destination

     bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic tamedia-topic-estimation

## List Kafka topics

    bin/kafka-topics.sh --list --zookeeper localhost:2181

## Delete Kafka topic

    bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic test

## Sending test data to Kafka topic using Kafka producer

     zcat stream.jsonl.gz | head -1000 | bin/kafka-console-producer.sh --broker-list localhost:9092 --topic tamedia-topic-source

## View topic stream
    
    bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic TOPIC_NAME --from-beginning

### Application localhost:8080
Receives JSON request containing:
     {  
      "jsonKey":"uid",
      "sourceTopic":"tamedia-topic-source",
      "destinationTopic":"tamedia-topic-destination",
      "bytes":"100",
      "seconds":"60"
     }
Values are mapped to KafkaRequest entity.

     api/print_stream
     Reads data from  Kafka topic, parse JSON(extract key, value) and sends topic items to stdout

     api/kafka_pipe
     Reads data from  Kafka topic, parse JSON(extract key, value) and sends creates data structures for counting uniqe json_key(uid) values
     Algorithms used for counting distinct elements in a stream: HashSet, HyperLogLog, Linear counting

     api/read_produce
     Reads data from  Kafka topic, parse JSON(extract key, value) and sends topic items to a new Kafka topic

     api/estimate_data
     Reads data from Kafka topic, and produces estimator from input stream, creates new JSON  {“ts”:<timestamp>, “range“:<range>,”ec”:<ecvalue>,”est”:<estimator>} and      writes this values in new Kafka topic

     api/get_estimator
     Reads estimated data from Kafka topic, and produces estimator from input stream, creates new JSON  {“ts”:<timestamp>,      “range“:<range>,”ec”:<ecvalue>,”est”:<estimator>} and writes this values in new Kafka topic

## How to compile/run Java program on Linux

Enter the following command to install cURL:

     sudo apt-get update
     sudo apt-get install curl

Start Spring Boot application

     mvn package
     sudo ln -s /path/to/your-app.jar /etc/init.d/your-app
     sudo service your-app start or /etc/init.d/myapp start

Created a symbolic link to your executable JAR file.

This link enables you to start the application as a service and writes console logs to /path/to/your-app/log/<appname>.log

Sending request to specific api

     curl --header "Content-Type: application/json" \
     --request GET\
     --data '{  
     "jsonKey":"uid",
     "sourceTopic":"tamedia-topic-source",
     "destinationTopic":"tamedia-topic-destination",
     "bytes":"100",
     "seconds":"60"}' \
     http://localhost:8080/api/print_stream

#EXAMPLES

Produce test data to kafka topic, consume data, write data to stdout

     bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic tamedia-topic-source

     zcat stream.jsonl.gz | head -1000 | bin/kafka-console-producer.sh --broker-list localhost:9092 --topic tamedia-topic-source

     curl --header "Content-Type: application/json" \
     --request GET\
     --data '{  
     "jsonKey":"uid",
     "sourceTopic":"tamedia-topic-source",
     "destinationTopic":"tamedia-topic-destination",
     "bytes":"100",
     "seconds":"60"}' \
     http://localhost:8080/api/print_stream

Consume test data from source topic and produce it to destination topic 

     http://localhost:8080/api/read_produce

Count unique jsonKey values

     http://localhost:8080/api/kafka_pipe

Estimate test data and produces to destination topic

     http://localhost:8080/api/estimate_data

Read estimated data from previous destination-topic

     http://localhost:8080/api/get_estimator


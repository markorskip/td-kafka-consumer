## TD-Kafka-Consumer

### Kafka Consumer Template

This application runs a consumer kafka client and receives messages from it.
It then stores those message into a log file.

### Running local instance and testing
Install Kafka and cd to kafka directore

Run zookeeper: 
bin/zookeeper-server-start.sh config/zookeeper.properties

Run broker service:
$ bin/kafka-server-start.sh config/server.properties

Run this application - a consumer will start listening to topic1 (can be changed in application.properties)

From the Kafka directory run:
bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic topic1

Start sending test messages from the console - the application with print them to screen and add them
to messageLog.txt

### Automated testing
Run integration tests

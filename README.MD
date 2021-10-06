### Kafka Consumer Template

This application runs a consumer kafka client and receives messages from it.
It then stores those message into a log file.

### Manual testing
From the Kafka directory run:
bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic topic1


Send test messages - check they are written to the console

### Automated testing
Run integration test

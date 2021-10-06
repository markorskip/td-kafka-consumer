package com.example.tdkafkaconsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

@Component
public class KafkaConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumer.class);

    private CountDownLatch latch = new CountDownLatch(1);
    private String payload = null;

    @KafkaListener(topics = "${message.topic.name}")
    public void receive(ConsumerRecord<?, ?> consumerRecord) throws IOException {
        LOGGER.info("received payload='{}'", consumerRecord.toString());
        setPayload(consumerRecord.toString());
        writeToMessageLog(consumerRecord.value().toString());
        latch.countDown();
    }

    private void writeToMessageLog(String message) throws IOException {
        FileWriter fw = new FileWriter("messageLog.txt", true);
        BufferedWriter bw = new BufferedWriter(fw);
        bw.write(message);
        bw.newLine();
        bw.close();
    }

    public CountDownLatch getLatch() {
        return latch;
    }

    public String getPayload() {
        return payload;
    }

    private void setPayload(String payload) {
        this.payload = payload;
    }

}

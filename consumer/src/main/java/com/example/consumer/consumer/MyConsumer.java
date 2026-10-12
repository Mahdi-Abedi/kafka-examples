package com.example.consumer.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class MyConsumer {

    @KafkaListener(topics = "my-topic", groupId = "my-consumer-group")
    protected void kafkaListener(String message) {
        log.info("Received message: {}", message);
    }
}

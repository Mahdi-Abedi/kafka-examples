package com.example.consumer.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.function.Consumer;

@Configuration
@Slf4j
public class MyConsumer {

    @Bean
    protected Consumer<String> myMessageCreated() {
        return message -> System.out.println("Received message: " + message);
    }
}

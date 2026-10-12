package com.example.producer;

import com.example.producer.services.MyProducerService;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.stream.Stream;

@SpringBootApplication
public class ProducerApplication {

    public static void main(String[] args) {
        SpringApplication.run(ProducerApplication.class, args);
    }

    @Bean
    protected CommandLineRunner commandLineRunner(MyProducerService myProducerService) throws InterruptedException {
        return _ -> myProducerService.sendMessage("my-topic" , "This is a message from producer ...");
    }
}

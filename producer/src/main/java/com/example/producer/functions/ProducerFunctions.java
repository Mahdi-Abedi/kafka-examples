package com.example.producer.functions;

import lombok.RequiredArgsConstructor;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class ProducerFunctions {

    private final StreamBridge streamBridge;

    public void sendMyMessage() {
        String message = "message" + UUID.randomUUID();
        streamBridge.send("myMessageCreated-out-0", message);
        System.out.println("Sending message : " + message);
    }
}

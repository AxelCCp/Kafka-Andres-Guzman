package com.course.kafka.api.models.services;

import com.course.kafka.api.models.Command;
import com.course.kafka.api.models.dto.ProductDto;

import java.util.UUID;

import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;

@Service
public class ProductCommandServiceImpl implements ProductCommandService {
    private final StreamBridge bridge;

    public ProductCommandServiceImpl(StreamBridge bridge) {
        this.bridge = bridge;
    }

    @Override
    public void sendCreate(ProductDto productDto) {
        Command<ProductDto> cmd = new Command<>("CREATE", null, productDto);
        String correlationId = UUID.randomUUID().toString();
        Message<Command<ProductDto>>msg = MessageBuilder.withPayload(cmd).setHeader("correlationId", correlationId).build();
        //boolean sent = this.bridge.send("commands-out-0", cmd);
        boolean sent = this.bridge.send("commands-out-0", msg);
        if(!sent) {
            throw new IllegalStateException("No se pudo enviar el comando a kafka.");
        }
    }
}

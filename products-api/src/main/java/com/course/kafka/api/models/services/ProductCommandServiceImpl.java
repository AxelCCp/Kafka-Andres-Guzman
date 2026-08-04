package com.course.kafka.api.models.services;

import com.course.kafka.api.models.Command;
import com.course.kafka.api.models.dto.ProductDto;
import org.springframework.cloud.stream.function.StreamBridge;
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
        boolean sent = this.bridge.send("commands-out-0", cmd);
        if(!sent) {
            throw new IllegalStateException("No se pudo enviar el comando a kafka.");
        }
    }
}

package com.course.kafka.api.models.services;

import com.course.kafka.api.messaging.ReplyInbox;
import com.course.kafka.api.models.Command;
import com.course.kafka.api.models.CommandType;
import com.course.kafka.api.models.Reply;
import com.course.kafka.api.models.dto.ProductDto;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;

@Service
public class ProductCommandServiceImpl implements ProductCommandService {

    private final StreamBridge bridge;
    private final ReplyInbox replyInbox;
    private static final Logger logger = LoggerFactory.getLogger(ProductCommandServiceImpl.class);

    public ProductCommandServiceImpl(StreamBridge bridge, ReplyInbox replyInbox) {
        this.bridge = bridge;
        this.replyInbox = replyInbox;
    }

    /*
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
    */

    @Override
    public Reply<?> SendCreateAndAwait(ProductDto productDto, Duration timeout) {
        Command<ProductDto> cmd = new Command<>(CommandType.CREATE, null, productDto);
        return sendAndAwait(timeout, cmd); 
    }

    @Override
    public Reply<?> sendReadAndAwait(Long id, Duration timeout) {
        Command<ProductDto> cmd = new Command<>(CommandType.READ, id, null);
        return sendAndAwait(timeout, cmd); 
    }


    private Reply<?> sendAndAwait(Duration timeout, Command<?> cmd) {
        
        String correlationId = UUID.randomUUID().toString();

        logger.info("Api products client - creating product with correlationID={}", correlationId);

        CompletableFuture<Reply<?>> future = replyInbox.register(correlationId);

        Message<?>msg = MessageBuilder.withPayload(cmd).setHeader("correlationId", correlationId).build();

        //boolean sent = this.bridge.send("commands-out-0", cmd);
        boolean sent = this.bridge.send("commands-out-0", msg);
        
        if(!sent) {
            throw new IllegalStateException("No se pudo enviar el comando a kafka.");
        }

        try {
            return  future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException("timeout... esperando respuesta de products-command desde kafka",e);       
        }
    }

    @Override
    public Reply<?> sendReadAllAndAwait(Duration timeout) {
        Command<Object>cmd = new Command<>(CommandType.READ_ALL, null, null);
        return this.sendAndAwait(timeout, cmd);
    }

    @Override
    public Reply<?> sendUpdateAllAndAwait(Long id, ProductDto dto, Duration timeout) {
        Command<ProductDto>cmd = new Command<>(CommandType.UPDATE, id, dto);
        return sendAndAwait(timeout, cmd);
    }

    @Override
    public Reply<?> sendDeleteAndAwait(Long id, Duration timeout) {
        Command<Object>cmd = new Command<>(CommandType.DELETE, id, null);
        return sendAndAwait(timeout, cmd);
    }
}

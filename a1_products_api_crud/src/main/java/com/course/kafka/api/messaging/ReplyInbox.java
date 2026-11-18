package com.course.kafka.api.messaging;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Component;

import com.course.kafka.api.models.Reply;

@Component 
public class ReplyInbox {

    //35
    private final ConcurrentHashMap<String, CompletableFuture<Reply<?>>>pending = new ConcurrentHashMap<>();

    public CompletableFuture<Reply<?>>register (String correlationId) {
        CompletableFuture<Reply<?>>future = new CompletableFuture<>();
        pending.put(correlationId, future);
        return future;
    }

    public void complete(String correlationId, Reply<?>reply) {
        if(correlationId==null) {
            throw new NullPointerException("Correlation id cant be null");
        }
        CompletableFuture<Reply<?>> future = pending.remove(correlationId);
        if(future != null) {
            future.complete(reply);
        }
    }

}

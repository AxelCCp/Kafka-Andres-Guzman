package com.course.kafka.api.handler;

import java.util.function.Consumer;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;

import com.course.kafka.api.messaging.ReplyInbox;
import com.course.kafka.api.models.Reply;

//36
@Configuration
public class RepliesConsumer {

    private final ReplyInbox replyInbox;
    
    public RepliesConsumer(ReplyInbox replyInbox) {
        this.replyInbox = replyInbox;
    }

    @Bean
    public Consumer<Message<Reply<?>>>handleReplies() {
        return message -> {
            String correlationId = message.getHeaders().get("correlationId", String.class);
            replyInbox.complete(correlationId, message.getPayload());
        };
    }



}

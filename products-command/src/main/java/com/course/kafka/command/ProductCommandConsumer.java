package com.course.kafka.command;

import com.course.kafka.command.models.Command;
import com.course.kafka.command.models.dto.ProductDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.function.Consumer;

@Configuration
public class ProductCommandConsumer {

    private static final Logger log =  LoggerFactory.getLogger(ProductCommandConsumer.class);

    @Bean
    public Consumer<Command<ProductDto>> handlerCommands(){
        return command -> {
            log.info("comando recibido y consumido con exito:  type: {}, body: {}", command.type(), command.body());
        };
    }
}

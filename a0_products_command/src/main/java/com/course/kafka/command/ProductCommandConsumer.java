package com.course.kafka.command;

import com.course.kafka.command.models.Command;
import com.course.kafka.command.models.Reply;
import com.course.kafka.command.models.dto.ProductDto;
import com.course.kafka.command.models.service.ProductService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.util.function.Consumer;
import java.util.function.Function;

@Configuration
public class ProductCommandConsumer {

    private static final Logger log =  LoggerFactory.getLogger(ProductCommandConsumer.class);

    private final ProductService productService;

    public ProductCommandConsumer(ProductService productService) {
        this.productService = productService;
    }


    //@Bean
    //public Function<Command<ProductDto>, Reply<?>> handlerCommands(){

    //ahora se recibe un correlation id , por eso se cambia a message
    @Bean
    public Function<Message<Command<ProductDto>>, Message<Reply<?>>> handlerCommands(){

        return msg -> {

            Command<ProductDto>cmd = msg.getPayload();

            log.info("comando recibido y consumido con exito:  type: {}, body: {}", cmd.type(), cmd.body());

            String type = cmd.type() == null ? "" : cmd.type().toUpperCase();

            Reply<ProductDto>reply;

            log.info("***type={}", type);

            switch(type) {

                case "CREATE" -> {

                    if(cmd.body() == null) {
                        log.warn("Create empty body");
                        reply = new Reply<>("ERROR", "create empty body", null);
                    }

                    ProductDto productDto = cmd.body();
                    ProductDto dto_db = this.productService.create(productDto);

                    log.info("*** creating product name={}, price={}", dto_db.name(), dto_db.price());
                    reply = new Reply<>("Success", "Create product name", dto_db);
                }

                /* 
                case "UPDATE" -> {
                    log.info("*** updating product name={}, price={}", null, null);
                }

                case "DELETE" -> {
                    log.info("*** deleting product");
                }

                case "FIND_ALL" -> {
                    log.info("listando los productos");
                }

                case "READ_ONE" -> {
                    log.info("obteniendo producto");
                }
                */

                default -> {
                    log.warn("*** ENTRO  EN EL DEFAULT ***");
                    reply = new Reply<>("ERROR", "unkdown command type", null);
                }

            }

            String correlationId = msg.getHeaders().get("correlationId").toString();

            log.info("*** recibiendo correlationId={}", correlationId);

            MessageBuilder<Reply<?>>out = MessageBuilder.withPayload(reply);

            if(correlationId != null) {

                out.setHeader("correlationId", correlationId);
            
            }
        
            return out.build();
        };
    }
}

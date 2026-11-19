package com.course.kafka.command;

import com.course.kafka.command.models.Command;
import com.course.kafka.command.models.CommandType;
import com.course.kafka.command.models.Reply;
import com.course.kafka.command.models.ReplyStatus;
import com.course.kafka.command.models.dto.ProductDto;
import com.course.kafka.command.models.service.ProductService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;


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

            //String type = cmd.type() == null ? "" : cmd.type().toUpperCase();

            Reply<?>reply;

            log.info("***type={}", cmd.type());

            switch(cmd.type()) {

                case CommandType.CREATE -> {

                    if(cmd.body() == null) {
                        log.warn("Create empty body");
                        reply = new Reply<>(ReplyStatus.ERROR, "create empty body", null);
                    }

                    ProductDto productDto = cmd.body();
                    ProductDto dto_db = this.productService.create(productDto);

                    log.info("*** creating product name={}, price={}", dto_db.name(), dto_db.price());
                    reply = new Reply<>(ReplyStatus.SUCCESS, "Create product name", dto_db);

                }

                case CommandType.READ -> {

                    if(cmd.id() == null) {
                        log.warn("read empty id");
                        reply = new Reply<>(ReplyStatus.ERROR, "read empty id.... id required", null);
                    }
                    ProductDto dto = this.productService.findById(cmd.id());
                    reply = (dto == null) ? new Reply<>(ReplyStatus.SUCCESS, "product not found", null) : new Reply<>(ReplyStatus.SUCCESS, "Read product name", dto);

                    log.info("obteniendo producto por id");

                }

                case CommandType.READ_ALL -> {

                    reply = new Reply<>(ReplyStatus.SUCCESS, "Read all products list ", this.productService.findAll());
                    log.info("listando los productos");

                }

                case CommandType.UPDATE -> {
                    
                    if(cmd.body() == null || cmd.id() == null) {
                        log.warn("id and body is required!!!");
                        reply = new Reply<>(ReplyStatus.ERROR, "id and body is required", null);
                    }

                    ProductDto dto = this.productService.findById(cmd.id());
                    
                    if(dto != null) {
                        reply = new Reply<>(ReplyStatus.SUCCESS, "updated product ok", dto);
                        log.info("*** updating product name={}, price={}", dto.name(), dto.price());
                    } else {
                        reply = new Reply<>(ReplyStatus.ERROR, "product not found", null);
                        log.info("product not found ***");
                    }
                    
                }

                case CommandType.DELETE -> {

                    if(cmd.id() == null) {
                        log.warn("read empty id");
                        reply = new Reply<>(ReplyStatus.ERROR, "read empty id.... id required", null);
                    }
                    boolean result = productService.delete(cmd.id());

                    reply = result ? new Reply<>(ReplyStatus.SUCCESS, "deleting product", "deleted") : new Reply<>(ReplyStatus.ERROR, "NOT FOUND ..", null);

                    log.info("*** deleting product");

                }


                default -> {

                    log.warn("*** ENTRO  EN EL DEFAULT ***");
                    reply = new Reply<>(ReplyStatus.ERROR, "unkdown command type", null);

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

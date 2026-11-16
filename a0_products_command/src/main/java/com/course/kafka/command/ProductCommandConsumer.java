package com.course.kafka.command;

import com.course.kafka.command.models.Command;
import com.course.kafka.command.models.dto.ProductDto;
import com.course.kafka.command.models.service.ProductService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.function.Consumer;

@Configuration
public class ProductCommandConsumer {

    private static final Logger log =  LoggerFactory.getLogger(ProductCommandConsumer.class);

    private final ProductService productService;

    public ProductCommandConsumer(ProductService productService) {
        this.productService = productService;
    }


    @Bean
    public Consumer<Command<ProductDto>> handlerCommands(){
        return command -> {

            log.info("comando recibido y consumido con exito:  type: {}, body: {}", command.type(), command.body());

            String type = command.type() == null ? "" : command.type().toUpperCase();
            
            log.info("***type={}", type);

            switch(type) {

                case "CREATE" -> {

                    if(command.body() == null) {
                        log.warn("Create empty body");
                        return;
                    }

                    ProductDto productDto = command.body();
                    this.productService.create(productDto);

                    log.info("*** creating product name={}, price={}", productDto.name(), productDto.price());
                }

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

                default -> {
                    log.info("*** ENTRO  EN EL DEFAULT ***");
                }

            }
        };
    }
}

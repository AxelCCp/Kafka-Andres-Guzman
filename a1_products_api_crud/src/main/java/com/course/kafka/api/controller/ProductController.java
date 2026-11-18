package com.course.kafka.api.controller;

import com.course.kafka.api.models.Reply;
import com.course.kafka.api.models.dto.ProductDto;
import com.course.kafka.api.models.services.ProductCommandService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.util.Map;

@RestController
@RequestMapping("/products")
public class ProductController {

    private final ProductCommandService commandService;

    public ProductController(ProductCommandService commandService) {
        this.commandService = commandService;
    }

    @PostMapping
    public ResponseEntity<?>create(@Valid @RequestBody ProductDto productDto) {
        //this.commandService.sendCreate(productDto);
        Reply<?> reply = this.commandService.SendCreateAndAwait(productDto, Duration.ofSeconds(5));
        if("SUCCESS".equalsIgnoreCase(reply.status())) {
            return  ResponseEntity.ok(reply.body());
        }
        return ResponseEntity.badRequest().body(Map.of("error", reply.message()));
    }


}

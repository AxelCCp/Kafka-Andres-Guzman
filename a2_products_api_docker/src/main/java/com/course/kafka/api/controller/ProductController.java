package com.course.kafka.api.controller;

import com.course.kafka.api.models.Reply;
import com.course.kafka.api.models.ReplyStatus;
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

import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;



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
        return getRespEntity(reply);
    }


    @GetMapping("/{id}")
    public ResponseEntity<?> getById(@PathVariable Long id) {
        Reply<?> reply = this.commandService.sendReadAndAwait(id, Duration.ofSeconds(5));
        return getRespEntity(reply);
    }

    @GetMapping
    public ResponseEntity<?> getListAll() {
        Reply<?> reply = this.commandService.sendReadAllAndAwait(Duration.ofSeconds(5));
        return getRespEntity(reply);
    }

    @PutMapping("/{id}")
    public ResponseEntity<?>update(@PathVariable Long id, @Valid @RequestBody ProductDto productDto) {
        Reply<?> reply = this.commandService.sendUpdateAllAndAwait(id, productDto, Duration.ofSeconds(5));
        return getRespEntity(reply);
    }
    

    @DeleteMapping("/{id}")
     public ResponseEntity<?> deleteById(@PathVariable Long id) {
        Reply<?> reply = this.commandService.sendDeleteAndAwait(id, Duration.ofSeconds(5));
        return  getRespEntity(reply);
     }

    
    private ResponseEntity<?> getRespEntity(Reply<?> reply) {
        if(reply.status().isSuccess()) {
            return ResponseEntity.ok(reply.body());
        }
        return ResponseEntity.badRequest().body(Map.of("error", reply.message()));
    }
    

}

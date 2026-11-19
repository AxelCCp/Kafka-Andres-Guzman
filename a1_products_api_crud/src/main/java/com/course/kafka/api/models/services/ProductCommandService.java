package com.course.kafka.api.models.services;

import java.time.Duration;

import com.course.kafka.api.models.Reply;
import com.course.kafka.api.models.dto.ProductDto;

public interface ProductCommandService {
    //void sendCreate(ProductDto productDto);

    Reply<?>SendCreateAndAwait(ProductDto productDto, Duration timeout);

    Reply<?>sendReadAndAwait(Long id, Duration timeout);

    Reply<?>sendReadAllAndAwait(Duration timeout);

    Reply<?>sendUpdateAllAndAwait(Long id, ProductDto dto, Duration timeout);

    Reply<?>sendDeleteAndAwait(Long id, Duration timeout);

}

package com.course.kafka.api.models.services;

import com.course.kafka.api.models.dto.ProductDto;

public interface ProductCommandService {
    void sendCreate(ProductDto productDto);
}

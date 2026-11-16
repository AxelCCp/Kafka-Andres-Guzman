package com.course.kafka.command.models.service;

import com.course.kafka.command.models.dto.ProductDto;

public interface ProductService {
    ProductDto create(ProductDto dto);
}

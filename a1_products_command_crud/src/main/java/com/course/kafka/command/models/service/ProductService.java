package com.course.kafka.command.models.service;

import java.util.List;

import com.course.kafka.command.models.dto.ProductDto;

public interface ProductService {
    
    ProductDto create(ProductDto dto);

    ProductDto findById(Long id);

    List<ProductDto> findAll();

    ProductDto update(Long id, ProductDto dto);

    boolean delete(Long id);

}

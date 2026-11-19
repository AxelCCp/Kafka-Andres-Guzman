package com.course.kafka.command.models.mapper;

import com.course.kafka.command.entities.Product;
import com.course.kafka.command.models.dto.ProductDto;

public final class Mapper {

    private Mapper() {}

    static public ProductDto toDto(Product product){
        return new ProductDto(product.getId(), product.getName(), product.getPrice());
    }

    static public Product toEntity(ProductDto productDto) {
        Product entity = new Product(productDto.name(), productDto.price());
        entity.setId(productDto.id());
        return entity;
    }

}

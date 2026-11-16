package com.course.kafka.command.models.service;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.course.kafka.command.entities.Product;
import com.course.kafka.command.models.dto.ProductDto;
import com.course.kafka.command.models.repo.ProductRepo;

@Service 
public class ProductServiceImpl implements ProductService{

    private final ProductRepo productRepo;

    public ProductServiceImpl(ProductRepo productRepo) {
        this.productRepo = productRepo;
    }

    @Override
    @Transactional 
    public ProductDto create(ProductDto dto) {
        Product p = new Product(dto.name(), dto.price());
        Product p_db = this.productRepo.save(p);
        return new ProductDto(p_db.getId(), p_db.getName(), p_db.getPrice());
    }

}

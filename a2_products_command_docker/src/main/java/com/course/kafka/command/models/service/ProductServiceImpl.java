package com.course.kafka.command.models.service;

import java.util.List;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.course.kafka.command.entities.Product;
import com.course.kafka.command.models.dto.ProductDto;
import com.course.kafka.command.models.mapper.Mapper;
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
        //Product p = new Product(dto.name(), dto.price());
        //Product p_db = this.productRepo.save(p);
        //return new ProductDto(p_db.getId(), p_db.getName(), p_db.getPrice());
        Product p_db = this.productRepo.save(Mapper.toEntity(dto));
        return Mapper.toDto(p_db);
    }

    @Override
    @Transactional(readOnly = true)
    public ProductDto findById(Long id) {
        return this.productRepo.findById(id).map(p -> Mapper.toDto(p)).orElse(null);
    }

    @Override
    @Transactional(readOnly = true)
    public List<ProductDto> findAll() {
        return ((List<Product>)this.productRepo.findAll()).stream().map(p -> Mapper.toDto(p)).toList();
    }

    @Override
    @Transactional 
    public ProductDto update(Long id, ProductDto dto) {
        Product entity = this.productRepo.findById(id).orElse(null);
        if(entity == null) {
            return  null;
        }
        entity.setName(dto.name());
        entity.setPrice(dto.price());
        Product p_db = this.productRepo.save(entity);
        return Mapper.toDto(p_db);
    }

    @Override
    @Transactional 
    public boolean delete(Long id) {
        boolean result = this.productRepo.existsById(id);
        if(result) {
            this.productRepo.deleteById(id);
            return  true;
        }
        return false;
    }



}

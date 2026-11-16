package com.course.kafka.command.models.repo;

import org.springframework.data.repository.CrudRepository;

import com.course.kafka.command.entities.Product;

public interface ProductRepo extends CrudRepository <Product, Long>{}

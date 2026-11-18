package com.course.kafka.api.models;

public record Reply <T>(String status, String message, T body) {}
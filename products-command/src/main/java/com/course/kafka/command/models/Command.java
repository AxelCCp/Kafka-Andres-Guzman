package com.course.kafka.command.models;

public record Command <T>(String type, Long id, T body) {
}

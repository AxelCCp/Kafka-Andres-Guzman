package com.course.kafka.command.models.dto;

public record Reply <T>(String status, String message, T body) {

}

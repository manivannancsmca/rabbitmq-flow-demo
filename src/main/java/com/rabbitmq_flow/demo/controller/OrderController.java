package com.rabbitmq_flow.demo.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.rabbitmq_flow.demo.dto.ApiResponse;
import com.rabbitmq_flow.demo.dto.OrderMessageDto;
import com.rabbitmq_flow.demo.producer.OrderMessageProducer;

@RestController
@RequestMapping("/api/v1/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderMessageProducer producer;

    @PostMapping
    public ResponseEntity<ApiResponse<String>> submitOrder(@Valid @RequestBody OrderMessageDto dto) {
        producer.sendOrderMessage(dto);
        return ResponseEntity.accepted()
                .body(ApiResponse.success("Order request accepted for processing", dto.eventId()));
    }
}

package com.rabbitmq_flow.demo.service;

import org.springframework.stereotype.Service;

import com.rabbitmq_flow.demo.dto.OrderMessageDto;
import com.rabbitmq_flow.demo.exception.InvalidMessageException;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
public class OrderProcessingService {

    private final IdempotencyService idempotencyService;

    public void processOrder(OrderMessageDto dto) {
        if (idempotencyService.isDuplicate(dto.eventId())) {
            log.warn("Idempotency check triggered: Event ID {} was already processed. Skipping.", dto.eventId());
            return;
        }

        // Business Rule simulation: fail on negative values
        if (dto.amount() <= 0) {
            throw new InvalidMessageException("Invalid order amount: " + dto.amount());
        }

        log.info("Processing order business logic. OrderID: {}, Amount: ${}", dto.orderId(), dto.amount());

        // Mark as processed upon successful completion
        idempotencyService.markProcessed(dto.eventId());
    }
}

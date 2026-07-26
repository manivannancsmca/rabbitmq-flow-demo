package com.rabbitmq_flow.demo.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

public record OrderMessageDto(
    @NotBlank(message = "Event ID is required for idempotency")
    String eventId,

    @NotBlank(message = "Order ID cannot be empty")
    String orderId,

    @NotNull(message = "Amount is required")
    @Positive(message = "Amount must be strictly positive")
    Double amount
) {}

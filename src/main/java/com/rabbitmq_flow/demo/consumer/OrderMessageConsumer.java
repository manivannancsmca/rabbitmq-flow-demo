package com.rabbitmq_flow.demo.consumer;

import java.util.List;
import java.util.Map;

import org.slf4j.MDC;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import com.rabbitmq_flow.demo.dto.OrderMessageDto;
import com.rabbitmq_flow.demo.exception.InvalidMessageException;
import com.rabbitmq_flow.demo.service.OrderProcessingService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderMessageConsumer {

    private final OrderProcessingService orderProcessingService;

    // 1. Primary Consumer (uses default rabbitListenerContainerFactory with 3
    // retries)
    @RabbitListener(queues = "${app.rabbitmq.queue}")
    public void consumeOrderMessage(
            OrderMessageDto payload,
            Message amqpMessage,
            @Header(value = "x-correlation-id", required = false) String correlationId) {

        if (correlationId != null) {
            MDC.put("correlationId", correlationId);
        }

        try {
            log.info("Consumed message. Event ID: {}, Order ID: {}", payload.eventId(), payload.orderId());
            orderProcessingService.processOrder(payload);
        } finally {
            MDC.remove("correlationId");
        }
    }

    // 2. Dead Letter Queue Listener (uses isolated dlqContainerFactory — NO retry
    // loop)
    @RabbitListener(queues = "${app.rabbitmq.dlq-queue}", containerFactory = "dlqContainerFactory")
    public void consumeDeadLetterMessage(Message failedMessage) {
        try {
            log.error("DLQ Alert: Failed message caught! Content: {}", new String(failedMessage.getBody()));

            Map<String, Object> headers = failedMessage.getMessageProperties().getHeaders();
            if (headers.containsKey("x-death")) {
                log.error("DLQ x-death headers: {}", headers.get("x-death"));
            }
        } catch (Exception e) {
            // Log cleanly — do NOT throw exceptions inside DLQ handlers
            log.error("Error inside DLQ consumer execution: {}", e.getMessage());
        }
    }
}

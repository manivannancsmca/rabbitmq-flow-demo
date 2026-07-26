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
        } catch (InvalidMessageException e) {
            log.error("Validation error during processing: {}. Message sent to DLQ automatically.", e.getMessage());
            // Rethrowing causes the configured retry engine to attempt retries.
            // When attempts are exhausted, the message lands in DLQ.
            throw e;
        } catch (Exception e) {
            log.error("System error processing message: {}", e.getMessage(), e);
            throw e;
        } finally {
            MDC.remove("correlationId");
        }
    }

    // Monitoring listener attached to the Dead Letter Queue
    @RabbitListener(queues = "${app.rabbitmq.dlq-queue}")
    public void consumeDeadLetterMessage(Message failedMessage) {
        Map<String, Object> headers = failedMessage.getMessageProperties().getHeaders();
        log.error("DLQ Consumer Alert: Message landed in Dead Letter Queue! Content: {}", 
                new String(failedMessage.getBody()));

        if (headers.containsKey("x-death")) {
            List<Map<String, Object>> xDeath = (List<Map<String, Object>>) headers.get("x-death");
            if (!xDeath.isEmpty()) {
                Map<String, Object> deathInfo = xDeath.get(0);
                log.error("DLQ Reason: {}, Route count: {}", deathInfo.get("reason"), deathInfo.get("count"));
            }
        }
    }
}

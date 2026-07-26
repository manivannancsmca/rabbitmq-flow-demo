package com.rabbitmq_flow.demo.producer;

import java.util.UUID;

import org.slf4j.MDC;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.rabbitmq_flow.demo.dto.OrderMessageDto;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderMessageProducer {

    private final RabbitTemplate rabbitTemplate;

    @Value("${app.rabbitmq.exchange}")
    private String exchange;

    @Value("${app.rabbitmq.routing-key}")
    private String routingKey;

    public void sendOrderMessage(OrderMessageDto payload) {
        String correlationId = MDC.get("correlationId");

        if (correlationId == null) {
            correlationId = UUID.randomUUID().toString();
        }

        CorrelationData correlationData = new CorrelationData(correlationId);

        final String finalCorrelationId = correlationId;
        log.info("Publishing message to exchange '{}' with routing key '{}'", exchange, routingKey);

        rabbitTemplate.convertAndSend(exchange, routingKey, payload, message -> {
            MessageProperties properties = message.getMessageProperties();
            properties.setCorrelationId(finalCorrelationId);
            properties.setHeader("x-correlation-id", finalCorrelationId);
            return message;
        }, correlationData);

    }
}

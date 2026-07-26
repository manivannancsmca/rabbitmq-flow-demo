package com.rabbitmq_flow.demo.config;

import java.util.HashMap;
import java.util.Map;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import lombok.extern.slf4j.Slf4j;

@Configuration
@Slf4j
public class RabbitMQConfig {

    @Value("${app.rabbitmq.exchange}")
    private String exchange;

    @Value("${app.rabbitmq.queue}")
    private String queue;

    @Value("${app.rabbitmq.routing-key}")
    private String routingKey;

    @Value("${app.rabbitmq.dlx-exchange}")
    private String dlxExchange;

    @Value("${app.rabbitmq.dlq-queue}")
    private String dlqQueue;

    @Value("${app.rabbitmq.dlq-routing-key}")
    private String dlqRoutingKey;

    @Value("${app.rabbitmq.ttl-ms}")
    private int ttlMs;

    // 1. Dead Letter Exchange & Queue Setup
    @Bean
    public DirectExchange deadLetterExchange() {
        return new DirectExchange(dlxExchange, true, false);
    }

    @Bean
    public Queue deadLetterQueue() {
        return QueueBuilder.durable(dlqQueue).build();
    }

    @Bean
    public Binding deadLetterBinding() {
        return BindingBuilder.bind(deadLetterQueue())
                .to(deadLetterExchange())
                .with(dlqRoutingKey);
    }

    // 2. Primary Exchange & Queue Setup with DLX routing
    @Bean
    public DirectExchange mainExchange() {
        return new DirectExchange(exchange, true, false);
    }

    @Bean
    public Queue mainQueue() {
        Map<String, Object> args = new HashMap<>();
        args.put("x-dead-letter-exchange", dlxExchange);
        args.put("x-dead-letter-routing-key", dlqRoutingKey);
        args.put("x-message-ttl", ttlMs);
        return QueueBuilder.durable(queue).withArguments(args).build();
    }

    @Bean
    public Binding mainBinding() {
        return BindingBuilder.bind(mainQueue())
                .to(mainExchange())
                .with(routingKey);
    }

    // 3. Serialization Protocol
    @Bean
    public Jackson2JsonMessageConverter jackson2JsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    // 4. Template with Publisher Confirm and Returns Logic
    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(jackson2JsonMessageConverter());

        // Confirm callback: verifies if message arrived at broker exchange
        template.setConfirmCallback((correlationData, ack, cause) -> {
            if (ack) {
                log.info("Publisher Confirm: Message successfully acknowledged by broker. ID: {}",
                        correlationData != null ? correlationData.getId() : "N/A");
            } else {
                log.error("Publisher Confirm: Message rejected by broker. Cause: {}, ID: {}",
                        cause, correlationData != null ? correlationData.getId() : "N/A");
            }
        });

        // Return callback: triggered if message is unroutable to any queue
        template.setReturnsCallback(returned -> {
            log.error("Publisher Return: Message unroutable. ReplyCode: {}, Text: {}, Exchange: {}, RoutingKey: {}",
                    returned.getReplyCode(), returned.getReplyText(), returned.getExchange(), returned.getRoutingKey());
        });

        return template;
    }
}

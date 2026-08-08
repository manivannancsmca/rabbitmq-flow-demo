package com.rabbitmq_flow.demo.config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.config.RetryInterceptorBuilder;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.retry.RejectAndDontRequeueRecoverer;
import org.springframework.amqp.support.converter.DefaultClassMapper;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class RabbitMQConfig {

    private static final Logger log = LoggerFactory.getLogger(RabbitMQConfig.class);

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

    // 3. Jackson JSON Message Converter
    @Bean
public Jackson2JsonMessageConverter jackson2JsonMessageConverter() {
    Jackson2JsonMessageConverter converter = new Jackson2JsonMessageConverter();
    
    // Create a type mapper to configure trusted packages
    DefaultClassMapper classMapper = new DefaultClassMapper();
    
    // Option 1 (Recommended): Trust your application's package
    classMapper.setTrustedPackages("com.rabbitmq_flow.demo.*", "com.rabbitmq_flow.demo.dto");
    
    // Option 2 (Alternative for dev): Trust all packages
    // classMapper.setTrustedPackages("*");
    
    converter.setClassMapper(classMapper);
    return converter;
}

    // 4. Primary Container Factory (3 Retries -> Reject without requeue -> Native DLX)
    @Bean(name = "rabbitListenerContainerFactory")
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(
            ConnectionFactory connectionFactory,
            Jackson2JsonMessageConverter jackson2JsonMessageConverter) {

        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setMessageConverter(jackson2JsonMessageConverter);

        // Retry 3 times, then RejectAndDontRequeueRecoverer sends basic.nack(requeue=false).
        // RabbitMQ natively moves the rejected message into the DLX.
        factory.setAdviceChain(
            RetryInterceptorBuilder.stateless()
                .maxAttempts(3)
                .backOffOptions(1000, 2.0, 10000)
                .recoverer(new RejectAndDontRequeueRecoverer())
                .build()
        );

        return factory;
    }

    // 5. Isolated DLQ Container Factory (NO retries, NO republishing)
    @Bean(name = "dlqContainerFactory")
    public SimpleRabbitListenerContainerFactory dlqContainerFactory(
            ConnectionFactory connectionFactory,
            Jackson2JsonMessageConverter jackson2JsonMessageConverter) {

        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setMessageConverter(jackson2JsonMessageConverter);
        // Explicitly zero retries attached here to prevent infinite DLQ loops
        return factory;
    }

    // 6. RabbitTemplate with Confirms & Returns
    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(jackson2JsonMessageConverter());

        template.setConfirmCallback((correlationData, ack, cause) -> {
            if (ack) {
                log.info("Publisher Confirm: Message successfully acknowledged by broker.");
            } else {
                log.error("Publisher Confirm: Message rejected by broker. Cause: {}", cause);
            }
        });

        template.setReturnsCallback(returned -> {
            log.error("Publisher Return: Message unroutable. Exchange: {}, RoutingKey: {}",
                    returned.getExchange(), returned.getRoutingKey());
        });

        return template;
    }
}

package com.rabbitmq_flow.demo.service;

import org.springframework.stereotype.Service;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class IdempotencyService {

    // Simple in-memory deduplication set. Use Redis in multi-instance production environments.
    private final Set<String> processedEvents = ConcurrentHashMap.newKeySet();

    public boolean isDuplicate(String eventId) {
        return processedEvents.contains(eventId);
    }

    public void markProcessed(String eventId) {
        processedEvents.add(eventId);
    }
}

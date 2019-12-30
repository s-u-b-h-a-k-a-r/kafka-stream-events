package com.kafka.streams.repository;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.kafka.streams.events.BaseEvent;
import com.kafka.streams.model.CreditCard;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class Repository {
    public static final String CREDIT_CARDS_EVENTS = "credit-cards-events";
    private final KafkaTemplate<String, BaseEvent> kafkaTemplate;
    private final Map<UUID, List<BaseEvent>> eventStreams = new ConcurrentHashMap<>();

    public Repository(KafkaTemplate<String, BaseEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void save(CreditCard creditCard) {
        List<BaseEvent> currentStream = eventStreams.getOrDefault(creditCard.getUuid(), new ArrayList<>());
        List<BaseEvent> newEvents = creditCard.getDirtyEvents();
        currentStream.addAll(newEvents);
        eventStreams.put(creditCard.getUuid(), currentStream);
        newEvents.forEach(domainEvent -> kafkaTemplate.send(CREDIT_CARDS_EVENTS, domainEvent));
        creditCard.eventsFlushed();
    }
}

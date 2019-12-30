package com.kafka.streams.repository;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.kafka.streams.events.DomainEvent;
import com.kafka.streams.model.CreditCard;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class Repository {
    public static final String S1P_CREDIT_CARDS_EVENTS = "s1p-credit-cards-events";
    private final KafkaTemplate<String, DomainEvent> kafkaTemplate;
    private final Map<UUID, List<DomainEvent>> eventStreams = new ConcurrentHashMap<>();

    public Repository(KafkaTemplate<String, DomainEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void save(CreditCard creditCard) {
        List<DomainEvent> currentStream = eventStreams.getOrDefault(creditCard.getUuid(), new ArrayList<>());
        List<DomainEvent> newEvents = creditCard.getDirtyEvents();
        currentStream.addAll(newEvents);
        eventStreams.put(creditCard.getUuid(), currentStream);
        newEvents.forEach(domainEvent -> kafkaTemplate.send(S1P_CREDIT_CARDS_EVENTS, domainEvent));
        creditCard.eventsFlushed();
    }
}

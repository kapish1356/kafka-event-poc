package com.example.kafkapoc.consumer;

import com.example.kafkapoc.dto.EventMessage;
import com.example.kafkapoc.entity.Event;
import com.example.kafkapoc.entity.EventStatus;
import com.example.kafkapoc.repository.EventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
public class EventConsumer {

    private static final Logger log = LoggerFactory.getLogger(EventConsumer.class);

    private final EventRepository repository;

    public EventConsumer(EventRepository repository) {
        this.repository = repository;
    }

    /**
     * Main listener. Throwing an exception triggers the retry + dead letter flow
     * configured in KafkaConfig.
     */
    @KafkaListener(topics = "${app.kafka.topics.events}", groupId = "${spring.kafka.consumer.group-id}")
    public void onEvent(EventMessage message) {
        log.info("Received event {} ({})", message.eventId(), message.title());

        // Idempotency: Kafka is at-least-once, so the same message can arrive twice.
        if (repository.existsByEventId(message.eventId())) {
            log.warn("Event {} already processed, skipping", message.eventId());
            return;
        }

        // Demo hook: a title containing "FAIL" simulates a processing error to show retries and the DLT.
        if (message.title() != null && message.title().toUpperCase().contains("FAIL")) {
            throw new IllegalStateException("Simulated processing failure for event " + message.eventId());
        }

        repository.save(new Event(
                message.eventId(), message.title(), message.description(), message.location(),
                message.eventDate(), message.publishedAt(), Instant.now(), EventStatus.PROCESSED));
        log.info("Saved event {} as PROCESSED", message.eventId());
    }

    /**
     * Dead letter listener: records that exhausted their retries end up here; we store them as FAILED.
     */
    @KafkaListener(topics = "${app.kafka.topics.events-dlt}", groupId = "${spring.kafka.consumer.group-id}-dlt")
    public void onDeadLetter(EventMessage message) {
        log.error("Dead letter received for event {} ({})", message.eventId(), message.title());
        if (repository.existsByEventId(message.eventId())) {
            return;
        }
        repository.save(new Event(
                message.eventId(), message.title(), message.description(), message.location(),
                message.eventDate(), message.publishedAt(), Instant.now(), EventStatus.FAILED));
    }
}

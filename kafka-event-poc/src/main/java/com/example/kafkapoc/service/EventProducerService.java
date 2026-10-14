package com.example.kafkapoc.service;

import com.example.kafkapoc.dto.EventMessage;
import com.example.kafkapoc.dto.EventRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

@Service
public class EventProducerService {

    private static final Logger log = LoggerFactory.getLogger(EventProducerService.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String eventsTopic;

    public EventProducerService(KafkaTemplate<String, Object> kafkaTemplate,
                                @Value("${app.kafka.topics.events}") String eventsTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.eventsTopic = eventsTopic;
    }

    /**
     * Publishes the event to Kafka and returns the generated eventId immediately.
     * The eventId is used as the record key, so all messages for one event land on the same partition.
     */
    public String publish(EventRequest request) {
        String eventId = UUID.randomUUID().toString();
        EventMessage message = new EventMessage(
                eventId,
                request.title(),
                request.description(),
                request.location(),
                request.eventDate(),
                Instant.now());

        kafkaTemplate.send(eventsTopic, eventId, message)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish event {}: {}", eventId, ex.getMessage());
                    } else {
                        log.info("Published event {} to {}-{} @ offset {}",
                                eventId,
                                result.getRecordMetadata().topic(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                });
        return eventId;
    }
}

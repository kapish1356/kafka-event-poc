package com.example.kafkapoc.dto;

import java.time.Instant;

/**
 * Payload that travels through Kafka (serialized as JSON).
 */
public record EventMessage(
        String eventId,
        String title,
        String description,
        String location,
        Instant eventDate,
        Instant publishedAt) {
}

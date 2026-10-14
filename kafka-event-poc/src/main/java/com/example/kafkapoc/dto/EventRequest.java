package com.example.kafkapoc.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.Instant;

/**
 * Request body for POST /api/events.
 */
public record EventRequest(
        @NotBlank(message = "title is required") String title,
        String description,
        @NotBlank(message = "location is required") String location,
        @NotNull(message = "eventDate is required (ISO-8601, e.g. 2026-12-01T10:00:00Z)") Instant eventDate) {
}

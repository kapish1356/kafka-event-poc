package com.example.kafkapoc.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name = "events")
public class Event {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private String eventId;

    @Column(nullable = false)
    private String title;

    @Column(length = 1000)
    private String description;

    private String location;

    private Instant eventDate;

    private Instant publishedAt;

    private Instant processedAt;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private EventStatus status;

    protected Event() {
    }

    public Event(String eventId, String title, String description, String location,
                 Instant eventDate, Instant publishedAt, Instant processedAt, EventStatus status) {
        this.eventId = eventId;
        this.title = title;
        this.description = description;
        this.location = location;
        this.eventDate = eventDate;
        this.publishedAt = publishedAt;
        this.processedAt = processedAt;
        this.status = status;
    }

    public Long getId() { return id; }
    public String getEventId() { return eventId; }
    public String getTitle() { return title; }
    public String getDescription() { return description; }
    public String getLocation() { return location; }
    public Instant getEventDate() { return eventDate; }
    public Instant getPublishedAt() { return publishedAt; }
    public Instant getProcessedAt() { return processedAt; }
    public EventStatus getStatus() { return status; }
}

package com.example.kafkapoc.controller;

import com.example.kafkapoc.dto.EventRequest;
import com.example.kafkapoc.entity.Event;
import com.example.kafkapoc.entity.EventStatus;
import com.example.kafkapoc.repository.EventRepository;
import com.example.kafkapoc.service.EventProducerService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/events")
public class EventController {

    private final EventProducerService producerService;
    private final EventRepository repository;

    public EventController(EventProducerService producerService, EventRepository repository) {
        this.producerService = producerService;
        this.repository = repository;
    }

    /** Publishes an event to Kafka. 202 = accepted for async processing (not yet saved in the DB). */
    @PostMapping
    public ResponseEntity<Map<String, String>> create(@Valid @RequestBody EventRequest request) {
        String eventId = producerService.publish(request);
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(Map.of("eventId", eventId, "message", "Event published to Kafka"));
    }

    /** Lists events saved by the consumer. Optional filter: ?status=PROCESSED or ?status=FAILED */
    @GetMapping
    public List<Event> list(@RequestParam(required = false) EventStatus status) {
        List<Event> all = repository.findAll();
        if (status == null) {
            return all;
        }
        return all.stream().filter(e -> e.getStatus() == status).toList();
    }

    @GetMapping("/{eventId}")
    public ResponseEntity<Event> get(@PathVariable String eventId) {
        return repository.findByEventId(eventId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
}

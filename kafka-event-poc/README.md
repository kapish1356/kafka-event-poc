# Kafka Event POC (Spring Boot + Kafka + H2)

A small proof of concept showing the core Kafka flow in Spring Boot:

```
POST /api/events ──► Producer ──► topic: events.created ──► Consumer ──► H2 (status PROCESSED)
                                          │
                                          └─ on failure: 3 attempts, then ─► events.created.DLT ──► DLT consumer ──► H2 (status FAILED)
```

**What it demonstrates**
- Producing JSON messages with `KafkaTemplate` (eventId used as the record key)
- Consuming with `@KafkaListener` and saving to H2 through Spring Data JPA
- Idempotent consumer (duplicate `eventId` is skipped, since Kafka is at-least-once)
- Retry (fixed backoff) + Dead Letter Topic via `DefaultErrorHandler`
- Poison-pill protection with `ErrorHandlingDeserializer`
- Topics created from code (`NewTopic` beans, 3 partitions each)

## Prerequisites
- JDK 17+
- Maven (or use IntelliJ's bundled Maven)
- Docker (only to run the Kafka broker)

## 1. Start Kafka
```bash
docker compose up -d
```
This starts a single-node Kafka (KRaft mode, no ZooKeeper) on `localhost:9092`.

## 2. Run the app
IntelliJ: **File > Open** the `kafka-event-poc` folder (pom.xml), wait for Maven import, run `KafkaEventPocApplication`.

Or from a terminal:
```bash
mvn spring-boot:run
```

## 3. Try it (Postman or curl)

**Publish an event**
```bash
curl -X POST http://localhost:8080/api/events \
  -H "Content-Type: application/json" \
  -d '{"title":"Tech Meetup","description":"Kafka basics","location":"Lucknow","eventDate":"2026-12-01T10:00:00Z"}'
```
Response (202 Accepted):
```json
{"eventId":"<uuid>","message":"Event published to Kafka"}
```

**List saved events**
```bash
curl http://localhost:8080/api/events
curl "http://localhost:8080/api/events?status=PROCESSED"
curl "http://localhost:8080/api/events?status=FAILED"
```

**Get one event**
```bash
curl http://localhost:8080/api/events/<eventId>
```

**Test the retry + dead letter flow**: send an event whose title contains `FAIL`:
```bash
curl -X POST http://localhost:8080/api/events \
  -H "Content-Type: application/json" \
  -d '{"title":"FAIL this one","location":"Delhi","eventDate":"2026-12-01T10:00:00Z"}'
```
In the logs you will see 3 failed attempts about 1 second apart, then "Dead letter received". After that,
`GET /api/events?status=FAILED` returns the event.

**Validation**: a request with a missing `title`, `location` or `eventDate` returns `400` with field errors.

## H2 console
http://localhost:8080/h2-console
- JDBC URL: `jdbc:h2:mem:eventdb`
- User: `sa`, password: empty

## Inspecting Kafka (optional)
```bash
# list topics
docker exec kafka-poc /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# watch messages on the main topic
docker exec kafka-poc /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic events.created --from-beginning

# watch the dead letter topic
docker exec kafka-poc /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic events.created.DLT --from-beginning
```

## Project structure
```
src/main/java/com/example/kafkapoc
├── KafkaEventPocApplication.java
├── config/KafkaConfig.java            topics + retry/DLT error handler
├── controller/EventController.java    REST API
├── controller/GlobalExceptionHandler  validation errors -> 400
├── service/EventProducerService.java  publishes to Kafka
├── consumer/EventConsumer.java        main listener + DLT listener
├── entity/Event, EventStatus          JPA entity
├── repository/EventRepository.java
└── dto/EventRequest, EventMessage     API payload / Kafka payload
```

## Configuration
Everything is in `src/main/resources/application.yml`: broker address, topic names, consumer group and retry
settings (`app.kafka.retry.attempts`, `app.kafka.retry.interval-ms`).

## Stop
```bash
docker compose down
```
The H2 database is in-memory, so data is cleared whenever the app restarts.

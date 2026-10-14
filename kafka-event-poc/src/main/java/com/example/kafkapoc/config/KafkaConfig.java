package com.example.kafkapoc.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfig {

    @Value("${app.kafka.topics.events}")
    private String eventsTopic;

    @Value("${app.kafka.topics.events-dlt}")
    private String eventsDltTopic;

    @Value("${app.kafka.retry.attempts}")
    private long retryAttempts;

    @Value("${app.kafka.retry.interval-ms}")
    private long retryIntervalMs;

    @Bean
    public NewTopic eventsTopic() {
        return TopicBuilder.name(eventsTopic).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic eventsDltTopic() {
        return TopicBuilder.name(eventsDltTopic).partitions(3).replicas(1).build();
    }

    /**
     * Retries a failing record a few times, then publishes it to the dead letter topic
     * (same partition number as the original record). Spring Boot wires this error handler
     * into the default listener container factory automatically.
     */
    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, ex) -> new TopicPartition(eventsDltTopic, record.partition()));
        // attempts = total retries after the first failure
        return new DefaultErrorHandler(recoverer, new FixedBackOff(retryIntervalMs, retryAttempts - 1));
    }
}

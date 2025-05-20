package com.recargapay.wallet.consumer.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Configures Kafka error handling for tests, including retries and DLQ publishing.
 */
@Configuration
public class TestKafkaErrorHandler {
    private static final Logger logger = LoggerFactory.getLogger(TestKafkaErrorHandler.class);

    /**
     * Creates a CommonErrorHandler with retry policies and DLQ publishing for tests.
     *
     * @param kafkaTemplate Kafka template for sending messages to the DLQ
     * @return Configured CommonErrorHandler
     */
    @Bean
    public CommonErrorHandler commonErrorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate, (record, ex) -> {
            logger.warn("Sending to DLQ: topic={}, partition={}, offset={}, key={}, error={}",
                    record.topic(), record.partition(), record.offset(), record.key(), ex.getMessage());
            return new org.apache.kafka.common.TopicPartition("wallet-events-dlq", record.partition());
        });
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3L));
        logger.info("CommonErrorHandler initialized with 3 retries and DLQ support");
        return errorHandler;
    }
}
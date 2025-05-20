package com.recargapay.wallet.consumer.config;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;

/**
 * Handles Kafka consumer errors, including deserialization and processing errors.
 * Retries failed messages up to a maximum number of attempts and sends unprocessable messages to a DLQ.
 */
public class KafkaErrorHandler extends DefaultErrorHandler {
    private static final Logger logger = LoggerFactory.getLogger(KafkaErrorHandler.class);

    /**
     * Configures the error handler with retry policies and DLQ publishing.
     *
     * @param kafkaTemplate Kafka template for sending messages to the DLQ
     */
    public KafkaErrorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
        super(createDeadLetterRecoverer(kafkaTemplate), new FixedBackOff(1000L, 3L));
        logger.info("KafkaErrorHandler initialized with 3 retries and DLQ support");
    }

    /**
     * Creates a DeadLetterPublishingRecoverer to send failed messages to the DLQ.
     *
     * @param kafkaTemplate Kafka template for DLQ publishing
     * @return Configured DeadLetterPublishingRecoverer
     */
    private static DeadLetterPublishingRecoverer createDeadLetterRecoverer(KafkaTemplate<String, Object> kafkaTemplate) {
        return new DeadLetterPublishingRecoverer(kafkaTemplate, (record, ex) -> {
            logger.error("Sending to DLQ: topic={}, partition={}, offset={}, key={}, value={}, error={}",
                    record.topic(), record.partition(), record.offset(), record.key(), record.value(), ex.getMessage(), ex);
            return new org.apache.kafka.common.TopicPartition("wallet-events-dlq", record.partition());
        });
    }

    /**
     * Handles the error by logging it before delegating to the default handler.
     */
    @Override
    public void handleRemaining(Exception thrownException, List<ConsumerRecord<?, ?>> records,
                                Consumer<?, ?> consumer, MessageListenerContainer container) {
        if (!records.isEmpty()) {
            ConsumerRecord<?, ?> record = records.get(0);
            logger.error("Error processing Kafka record: topic={}, partition={}, offset={}, key={}, value={}, error={}",
                    record.topic(), record.partition(), record.offset(), record.key(), record.value(), thrownException.getMessage(), thrownException);
        }
        super.handleRemaining(thrownException, records, consumer, container);
    }
}
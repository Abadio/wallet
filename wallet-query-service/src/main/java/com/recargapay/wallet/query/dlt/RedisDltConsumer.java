package com.recargapay.wallet.query.dlt;

import com.recargapay.wallet.common.event.DepositedEvent;
import com.recargapay.wallet.common.event.TransferredEvent;
import com.recargapay.wallet.common.event.WithdrawnEvent;
import com.recargapay.wallet.query.service.CacheService;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.concurrent.TimeUnit;

@Service
public class RedisDltConsumer {
    private static final Logger logger = LoggerFactory.getLogger(RedisDltConsumer.class);
    public static final String DLT_TOPIC = "wallet-query-dlt";
    public static final String FAILED_DLT_TOPIC = "wallet-query-dlt-failed";
    private static final String BALANCE_KEY_PREFIX = "wallet:balance:";

    private final CacheService cacheService;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final MeterRegistry meterRegistry;
    private final RedisTemplate<String, Object> redisTemplate;

    @Autowired
    public RedisDltConsumer(CacheService cacheService, KafkaTemplate<String, Object> kafkaTemplate,
                            MeterRegistry meterRegistry, RedisTemplate<String, Object> redisTemplate) {
        this.cacheService = cacheService;
        this.kafkaTemplate = kafkaTemplate;
        this.meterRegistry = meterRegistry;
        this.redisTemplate = redisTemplate;
    }

    @KafkaListener(
            topics = DLT_TOPIC,
            groupId = "${kafka.consumer.dlt.group-id}",
            containerFactory = "dltKafkaListenerContainerFactory"
    )
    public void consumeDlt(ConsumerRecord<String, Object> record, Acknowledgment acknowledgment) {
        // O listener apenas delega para a lógica de processamento.
        processDltRecord(record, acknowledgment);
    }

    // Método público que contém a lógica real, tornando-a testável.
    public void processDltRecord(ConsumerRecord<String, Object> record, Acknowledgment acknowledgment) {
        String key = record.key();
        Object value = record.value();
        String eventType = value != null ? value.getClass().getSimpleName() : "null";
        int retryCount = getRetryCount(record);

        logger.warn("Received DLT message: key={}, value={}, retryCount={}", key, eventType, retryCount);

        if (isAlreadyProcessed(record)) {
            logger.warn("Skipping already processed message: key={}", key);
            acknowledgment.acknowledge();
            return;
        }

        if (value == null) {
            handleProcessingError("null_event", null, key, acknowledgment);
            return;
        }

        if (Boolean.FALSE.equals(redisTemplate.hasKey(BALANCE_KEY_PREFIX + key))) {
            handleProcessingError("keys_expired", eventType, key, acknowledgment);
            return;
        }

        Header errorMessageHeader = record.headers().lastHeader("error-message");
        String errorMessage = errorMessageHeader != null ? new String(errorMessageHeader.value()) : "Unknown transient error";

        if(!isTransientError(errorMessage)) {
            handlePermanentError(key, eventType, "Non-transient error: " + errorMessage, record, acknowledgment);
            return;
        }

        try {
            if (value instanceof DepositedEvent event) {
                cacheService.invalidateCache(event.getWalletId());
            } else if (value instanceof WithdrawnEvent event) {
                cacheService.invalidateCache(event.getWalletId());
            } else if (value instanceof TransferredEvent event) {
                cacheService.invalidateCache(event.getFromWalletId());
                cacheService.invalidateCache(event.getToWalletId());
            } else {
                handleProcessingError("unsupported_event", eventType, key, acknowledgment);
                return;
            }
            logger.info("Cache successfully invalidated for eventType {} and key {}", eventType, key);
            acknowledgment.acknowledge();

        } catch (Exception e) {
            logger.warn("Failed to process event on attempt {}. Error: {}", retryCount + 1, e.getMessage());
            if (retryCount >= 2) {
                handlePermanentError(key, eventType, "Max retries reached. Last error: " + e.getMessage(), record, acknowledgment);
            } else {
                retryMessage(key, eventType, record, retryCount, e.getMessage(), acknowledgment);
            }
        }
    }

    private void handleProcessingError(String reason, String eventType, String key, Acknowledgment acknowledgment) {
        logger.warn("Discarding message. Reason: {}. Key: {}", reason, key);
        meterRegistry.counter("dlt.redis.discarded", "reason", reason, "eventType", eventType != null ? eventType : "unknown").increment();
        acknowledgment.acknowledge();
    }

    private boolean isAlreadyProcessed(ConsumerRecord<String, Object> record) {
        String messageId = getMessageId(record);
        String processedKey = "dlt:processed:" + messageId;
        try {
            return Boolean.FALSE.equals(redisTemplate.opsForValue().setIfAbsent(processedKey, "true", 5, TimeUnit.MINUTES));
        } catch (Exception e) {
            logger.error("Failed to check processed key in Redis. Assuming not processed and continuing.", e);
            return false;
        }
    }

    private void handlePermanentError(String key, String eventType, String errorMessage,
                                      ConsumerRecord<String, Object> record, Acknowledgment acknowledgment) {
        logger.warn("Permanent error, redirecting to failed DLT: key={}, type={}, error={}", key, eventType, errorMessage);
        meterRegistry.counter("dlt.redis.redirected", "eventType", eventType, "reason", "permanent_error").increment();
        ProducerRecord<String, Object> failedRecord = new ProducerRecord<>(FAILED_DLT_TOPIC, key, record.value());
        record.headers().forEach(failedRecord.headers()::add);
        failedRecord.headers().add("final-error-message", (errorMessage != null ? errorMessage : "Unknown").getBytes());
        try {
            kafkaTemplate.send(failedRecord);
        } finally {
            acknowledgment.acknowledge();
        }
    }

    private void retryMessage(String key, String eventType, ConsumerRecord<String, Object> record,
                              int retryCount, String errorMessage, Acknowledgment acknowledgment) {
        logger.warn("Scheduling retry #{} for key {}", retryCount + 1, key);
        meterRegistry.counter("dlt.redis.retries", "eventType", eventType).increment();
        ProducerRecord<String, Object> retryRecord = new ProducerRecord<>(DLT_TOPIC, key, record.value());
        record.headers().forEach(header -> {
            if (!header.key().equals("retry-count")) {
                retryRecord.headers().add(header);
            }
        });
        retryRecord.headers().add("retry-count", String.valueOf(retryCount + 1).getBytes());
        retryRecord.headers().add("error-message", errorMessage.getBytes());
        try {
            kafkaTemplate.send(retryRecord);
        } finally {
            acknowledgment.acknowledge();
        }
    }

    private String getMessageId(ConsumerRecord<String, Object> record) {
        Header messageIdHeader = record.headers().lastHeader("message-id");
        return messageIdHeader != null ? new String(messageIdHeader.value()) : record.topic() + "-" + record.partition() + "-" + record.offset();
    }

    private int getRetryCount(ConsumerRecord<String, Object> record) {
        Header retryCountHeader = record.headers().lastHeader("retry-count");
        if (retryCountHeader == null) return 0;
        try {
            return Integer.parseInt(new String(retryCountHeader.value()));
        } catch (NumberFormatException e) { return 0; }
    }

    private boolean isTransientError(String errorMessage) {
        return errorMessage != null && (errorMessage.contains("timeout") || errorMessage.contains("Connection refused") || errorMessage.contains("RedisConnectionException"));
    }

    public static String getDltTopic() { return DLT_TOPIC; }
    public static String getFailedDltTopic() { return FAILED_DLT_TOPIC; }
}
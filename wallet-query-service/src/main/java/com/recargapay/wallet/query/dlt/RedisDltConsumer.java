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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.concurrent.TimeUnit;

@Service
public class RedisDltConsumer {
    private static final Logger logger = LoggerFactory.getLogger(RedisDltConsumer.class);
    private static final String DLT_TOPIC = "wallet-query-dlt";
    private static final String FAILED_DLT_TOPIC = "wallet-query-dlt-failed";
    private static final String BALANCE_KEY_PREFIX = "wallet:balance:";

    private final CacheService cacheService;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final MeterRegistry meterRegistry;
    private final RedisTemplate<String, Object> redisTemplate;
    private final Map<Class<?>, DltEventProcessor> eventProcessors;
    private final String dltGroupId;

    @Autowired
    public RedisDltConsumer(CacheService cacheService, KafkaTemplate<String, Object> kafkaTemplate,
                            MeterRegistry meterRegistry, RedisTemplate<String, Object> redisTemplate,
                            @Value("${kafka.consumer.dlt.group-id}") String dltGroupId) {
        this.cacheService = cacheService;
        this.kafkaTemplate = kafkaTemplate;
        this.meterRegistry = meterRegistry;
        this.redisTemplate = redisTemplate;
        this.dltGroupId = dltGroupId;
        this.eventProcessors = initializeEventProcessors();
        logger.info("Initialized RedisDltConsumer with groupId: {}, eventProcessors: {}", dltGroupId, eventProcessors.keySet());
    }

    private Map<Class<?>, DltEventProcessor> initializeEventProcessors() {
        Map<Class<?>, DltEventProcessor> processors = new HashMap<>();
        processors.put(DepositedEvent.class, new DepositedEventProcessor());
        processors.put(TransferredEvent.class, new TransferredEventProcessor());
        processors.put(WithdrawnEvent.class, new WithdrawnEventProcessor());
        return processors;
    }

    @KafkaListener(
            topics = DLT_TOPIC,
            groupId = "${kafka.consumer.dlt.group-id}",
            containerFactory = "dltKafkaListenerContainerFactory"
    )
    public void consumeDlt(ConsumerRecord<String, Object> record, Acknowledgment acknowledgment) {
        String key = record.key();
        Object value = record.value();
        String eventType = value != null ? value.getClass().getSimpleName() : "null";
        Header retryCountHeader = record.headers().lastHeader("retry-count");
        int retryCount = retryCountHeader != null ? Integer.parseInt(new String(retryCountHeader.value())) : 0;
        Header messageIdHeader = record.headers().lastHeader("message-id");
        String messageId = messageIdHeader != null ? new String(messageIdHeader.value()) :
                (record.topic() + "-" + record.partition() + "-" + record.offset());

        // Verificar se a mensagem já foi processada
        String processedKey = "dlt:processed:" + messageId;
        logger.debug("Checking processed key: processedKey={}, messageId={}, key={}, retryCount={}",
                processedKey, messageId, key, retryCount);
        try {
            Boolean alreadyProcessed = redisTemplate.opsForValue().setIfAbsent(processedKey, "true", 120, TimeUnit.SECONDS);
            if (Boolean.FALSE.equals(alreadyProcessed)) {
                logger.warn("Skipping already processed message: key={}, messageId={}, retryCount={}", key, messageId, retryCount);
                acknowledgment.acknowledge();
                try {
                    Thread.sleep(500);
                    logger.debug("Waited after acknowledgment for processedKey={}", processedKey);
                } catch (InterruptedException ie) {
                    logger.error("Interrupted during acknowledgment sleep: {}", ie.getMessage(), ie);
                    Thread.currentThread().interrupt();
                }
                return;
            }
        } catch (Exception e) {
            logger.error("Failed to check/set processed key in Redis: key={}, messageId={}, error={}", key, messageId, e.getMessage(), e);
            handlePermanentError(key, eventType, "Redis failure during processed check: " + e.getMessage(), record, acknowledgment);
            return;
        }

        logger.warn("Received DLT message: key={}, value={}, retryCount={}, messageId={}", key, eventType, retryCount, messageId);

        try {
            if (value == null) {
                logger.warn("Null event received in DLT: key={}, messageId={}", key, messageId);
                meterRegistry.counter("dlt.redis.discarded", "reason", "null_event").increment();
                acknowledgment.acknowledge();
                try {
                    Thread.sleep(500);
                    logger.debug("Waited after acknowledgment for null event: messageId={}", messageId);
                } catch (InterruptedException ie) {
                    logger.error("Interrupted during acknowledgment sleep: {}", ie.getMessage(), ie);
                    Thread.currentThread().interrupt();
                }
                return;
            }

            String balanceKey = BALANCE_KEY_PREFIX + key;
            if (!redisTemplate.hasKey(balanceKey)) {
                logger.warn("Key {} expired, discarding message, messageId={}", balanceKey, messageId);
                meterRegistry.counter("dlt.redis.discarded", "reason", "keys_expired", "eventType", eventType).increment();
                acknowledgment.acknowledge();
                try {
                    Thread.sleep(500);
                    logger.debug("Waited after acknowledgment for expired key: messageId={}", messageId);
                } catch (InterruptedException ie) {
                    logger.error("Interrupted during acknowledgment sleep: {}", ie.getMessage(), ie);
                    Thread.currentThread().interrupt();
                }
                return;
            }

            DltEventProcessor processor = eventProcessors.get(value.getClass());
            if (processor == null) {
                logger.warn("Unsupported event type in DLT: key={}, type={}, messageId={}", key, eventType, messageId);
                meterRegistry.counter("dlt.redis.discarded", "reason", "unsupported_event").increment();
                acknowledgment.acknowledge();
                try {
                    Thread.sleep(500);
                    logger.debug("Waited after acknowledgment for unsupported event: messageId={}", messageId);
                } catch (InterruptedException ie) {
                    logger.error("Interrupted during acknowledgment sleep: {}", ie.getMessage(), ie);
                    Thread.currentThread().interrupt();
                }
                return;
            }

            Header errorMessageHeader = record.headers().lastHeader("error-message");
            String errorMessage = errorMessageHeader != null ? new String(errorMessageHeader.value()) : null;

            if (isTransientError(errorMessage)) {
                processor.processTransientError(key, eventType, errorMessage, record, retryCount, acknowledgment);
            } else {
                handlePermanentError(key, eventType, errorMessage, record, acknowledgment);
            }
        } catch (Exception e) {
            logger.error("Unexpected error processing DLT message: key={}, type={}, retryCount={}, messageId={}",
                    key, eventType, retryCount, messageId, e);
            handlePermanentError(key, eventType, e.getMessage(), record, acknowledgment);
        } finally {
            try {
                redisTemplate.expire(processedKey, 120, TimeUnit.SECONDS);
                logger.debug("Set expiration for processedKey={}", processedKey);
            } catch (Exception e) {
                logger.error("Failed to set expire on processed key {}: error={}", processedKey, e.getMessage(), e);
            }
        }
    }

    private void handlePermanentError(String key, String eventType, String errorMessage,
                                      ConsumerRecord<String, Object> record, Acknowledgment acknowledgment) {
        Header processed = record.headers().lastHeader("processed-once");
        if (processed != null && "true".equals(new String(processed.value()))) {
            logger.warn("Skipping duplicate redirection: key={}, type={}", key, eventType);
            acknowledgment.acknowledge();
            try {
                Thread.sleep(500); // Ensure acknowledgment is processed
            } catch (InterruptedException ie) {
                logger.error("Interrupted during acknowledgment sleep: {}", ie.getMessage(), ie);
                Thread.currentThread().interrupt();
            }
            return;
        }

        logger.warn("Permanent error detected, redirecting to failed DLT: key={}, type={}, error={}",
                key, eventType, errorMessage);
        meterRegistry.counter("dlt.redis.redirected", "eventType", eventType, "reason", "permanent_error").increment();
        ProducerRecord<String, Object> failedRecord = new ProducerRecord<>(FAILED_DLT_TOPIC, key, record.value());
        for (Header header : record.headers()) {
            failedRecord.headers().add(header);
        }
        failedRecord.headers().add("error-timestamp", OffsetDateTime.now().toString().getBytes());
        failedRecord.headers().add("error-message", errorMessage != null ? errorMessage.getBytes() : "Unknown error".getBytes());
        failedRecord.headers().add("processed-once", "true".getBytes());
        try {
            kafkaTemplate.send(failedRecord).get(10, TimeUnit.SECONDS);
            logger.warn("Successfully sent event to {}: key={}", FAILED_DLT_TOPIC, key);
        } catch (Exception e) {
            logger.error("Failed to send event to {}: key={}, error={}", FAILED_DLT_TOPIC, key, e.getMessage(), e);
        }
        acknowledgment.acknowledge();
        try {
            Thread.sleep(500); // Ensure acknowledgment is processed
        } catch (InterruptedException ie) {
            logger.error("Interrupted during acknowledgment sleep: {}", ie.getMessage(), ie);
            Thread.currentThread().interrupt();
        }
    }

    private boolean isTransientError(String errorMessage) {
        return errorMessage != null &&
                (errorMessage.contains("RedisConnectionException") ||
                        errorMessage.contains("JedisConnectionException") ||
                        errorMessage.contains("timeout") ||
                        errorMessage.contains("Connection refused"));
    }

    private interface DltEventProcessor {
        void processTransientError(String key, String eventType, String errorMessage,
                                   ConsumerRecord<String, Object> record, int retryCount, Acknowledgment acknowledgment);
    }

    private class DepositedEventProcessor implements DltEventProcessor {
        @Override
        public void processTransientError(String key, String eventType, String errorMessage,
                                          ConsumerRecord<String, Object> record, int retryCount, Acknowledgment acknowledgment) {
            logger.warn("Processing transient error for DepositedEvent: key={}, error={}, retryCount={}",
                    key, errorMessage, retryCount);

            try {
                DepositedEvent depositedEvent = (DepositedEvent) record.value();
                logger.warn("Invalidating cache for DepositedEvent: walletId={}", depositedEvent.getWalletId());
                cacheService.invalidateCache(depositedEvent.getWalletId());
                logger.info("Successfully invalidated cache for DepositedEvent: walletId={}", depositedEvent.getWalletId());
                acknowledgment.acknowledge();
                try {
                    Thread.sleep(500); // Aumentado para 500ms
                    logger.debug("Waited after acknowledgment for DepositedEvent: key={}", key);
                } catch (InterruptedException ie) {
                    logger.error("Interrupted during acknowledgment sleep: {}", ie.getMessage(), ie);
                    Thread.currentThread().interrupt();
                }
            } catch (Exception e) {
                logger.warn("Retry attempt {} failed for DepositedEvent: key={}, error={}", retryCount + 1, key, e.getMessage());
                if (retryCount >= 2) {
                    logger.warn("Max retries reached for DepositedEvent, redirecting to failed DLT: key={}", key);
                    handlePermanentError(key, eventType, e.getMessage(), record, acknowledgment);
                } else {
                    retryMessage(key, eventType, record, retryCount, e.getMessage(), acknowledgment);
                }
            }
        }
    }

    private class TransferredEventProcessor implements DltEventProcessor {
        @Override
        public void processTransientError(String key, String eventType, String errorMessage,
                                          ConsumerRecord<String, Object> record, int retryCount, Acknowledgment acknowledgment) {
            logger.warn("Processing transient error for TransferredEvent: key={}, error={}, retryCount={}",
                    key, errorMessage, retryCount);

            TransferredEvent transferredEvent = (TransferredEvent) record.value();
            UUID fromWalletId = transferredEvent.getFromWalletId();
            UUID toWalletId = transferredEvent.getToWalletId();
            logger.warn("Attempting to invalidate cache for TransferredEvent: fromWalletId={}, toWalletId={}",
                    fromWalletId, toWalletId);

            boolean failed = false;
            List<Exception> exceptions = new ArrayList<>();

            try {
                logger.info("Calling invalidateCache for fromWalletId={}", fromWalletId);
                cacheService.invalidateCache(fromWalletId);
                logger.info("Successfully invalidated cache for fromWalletId={}", fromWalletId);
            } catch (Exception e) {
                logger.error("Failed to invalidate cache for fromWalletId={}", fromWalletId, e);
                failed = true;
                exceptions.add(e);
            }

            try {
                logger.info("Calling invalidateCache for toWalletId={}", toWalletId);
                cacheService.invalidateCache(toWalletId);
                logger.info("Successfully invalidated cache for toWalletId={}", toWalletId);
            } catch (Exception e) {
                logger.error("Failed to invalidate cache for toWalletId={}", toWalletId, e);
                failed = true;
                exceptions.add(e);
            }

            if (!failed) {
                logger.info("Successfully invalidated caches for TransferredEvent: fromWalletId={}, toWalletId={}",
                        fromWalletId, toWalletId);
                acknowledgment.acknowledge();
                try {
                    Thread.sleep(500); // Aumentado para 500ms
                    logger.debug("Waited after acknowledgment for TransferredEvent: key={}", key);
                } catch (InterruptedException ie) {
                    logger.error("Interrupted during acknowledgment sleep: {}", ie.getMessage(), ie);
                    Thread.currentThread().interrupt();
                }
            } else {
                logger.warn("Retry attempt {} failed for TransferredEvent: key={}, errors={}", retryCount + 1, key,
                        exceptions.stream().map(Exception::getMessage).collect(Collectors.joining("; ")));
                String combinedErrorMessage = exceptions.stream()
                        .map(e -> (e.getMessage() != null ? e.getMessage() : "Unknown error"))
                        .collect(Collectors.joining(" | "));
                if (retryCount >= 2) {
                    logger.warn("Max retries reached for TransferredEvent, redirecting to failed DLT: key={}", key);
                    RuntimeException aggregateException = new RuntimeException(
                            "Failed to invalidate cache for TransferredEvent: fromWalletId=" + fromWalletId +
                                    ", toWalletId=" + toWalletId + "; errors=" + combinedErrorMessage
                    );
                    exceptions.forEach(aggregateException::addSuppressed);
                    handlePermanentError(key, eventType, aggregateException.getMessage(), record, acknowledgment);
                } else {
                    retryMessage(key, eventType, record, retryCount, combinedErrorMessage, acknowledgment);
                }
            }
        }
    }

    private class WithdrawnEventProcessor implements DltEventProcessor {
        @Override
        public void processTransientError(String key, String eventType, String errorMessage,
                                          ConsumerRecord<String, Object> record, int retryCount, Acknowledgment acknowledgment) {
            logger.warn("Processing transient error for WithdrawnEvent: key={}, error={}, retryCount={}",
                    key, errorMessage, retryCount);

            try {
                WithdrawnEvent withdrawnEvent = (WithdrawnEvent) record.value();
                logger.warn("Invalidating cache for WithdrawnEvent: walletId={}", withdrawnEvent.getWalletId());
                cacheService.invalidateCache(withdrawnEvent.getWalletId());
                logger.info("Successfully invalidated cache for WithdrawnEvent: walletId={}", withdrawnEvent.getWalletId());
                acknowledgment.acknowledge();
                try {
                    Thread.sleep(500); // Aumentado para 500ms
                    logger.debug("Waited after acknowledgment for WithdrawnEvent: key={}", key);
                } catch (InterruptedException ie) {
                    logger.error("Interrupted during acknowledgment sleep: {}", ie.getMessage(), ie);
                    Thread.currentThread().interrupt();
                }
            } catch (Exception e) {
                logger.warn("Retry attempt {} failed for WithdrawnEvent: key={}, error={}", retryCount + 1, key, e.getMessage());
                if (retryCount >= 2) {
                    logger.warn("Max retries reached for WithdrawnEvent, redirecting to failed DLT: key={}", key);
                    handlePermanentError(key, eventType, e.getMessage(), record, acknowledgment);
                } else {
                    retryMessage(key, eventType, record, retryCount, e.getMessage(), acknowledgment);
                }
            }
        }
    }

    private void retryMessage(String key, String eventType, ConsumerRecord<String, Object> record,
                              int retryCount, String errorMessage, Acknowledgment acknowledgment) {
        String messageId = record.topic() + "-" + record.partition() + "-" + record.offset();
        String retryKey = "dlt:retry:" + messageId + ":" + retryCount;

        logger.debug("Checking idempotency for message: key={}, retryCount={}, messageId={}, retryKey={}",
                key, retryCount, messageId, retryKey);

        try {
            if (redisTemplate.getConnectionFactory() == null || redisTemplate.getConnectionFactory().getConnection() == null) {
                logger.error("Redis connection is not available for message: key={}, retryCount={}, messageId={}",
                        key, retryCount, messageId);
                throw new IllegalStateException("Redis connection unavailable");
            }

            Boolean alreadyRetried = redisTemplate.opsForValue().setIfAbsent(retryKey, "true", 120, TimeUnit.SECONDS);
            logger.debug("setIfAbsent result for retryKey={}: {}", retryKey, alreadyRetried);
            if (Boolean.FALSE.equals(alreadyRetried)) {
                logger.warn("Skipping duplicate retry for message: key={}, retryCount={}, messageId={}, retryKey={}",
                        key, retryCount, messageId, retryKey);
                acknowledgment.acknowledge();
                try {
                    Thread.sleep(500);
                    logger.debug("Waited after acknowledgment for duplicate retry: messageId={}", messageId);
                } catch (InterruptedException ie) {
                    logger.error("Interrupted during acknowledgment sleep: {}", ie.getMessage(), ie);
                    Thread.currentThread().interrupt();
                }
                return;
            }

            ProducerRecord<String, Object> retryRecord = new ProducerRecord<>(DLT_TOPIC, key, record.value());
            for (Header header : record.headers()) {
                if (!header.key().equals("retry-count") && !header.key().equals("error-message")) {
                    retryRecord.headers().add(header);
                }
            }
            int newRetryCount = retryCount + 1;
            retryRecord.headers().add("retry-count", String.valueOf(newRetryCount).getBytes());
            retryRecord.headers().add("error-message", errorMessage.getBytes());
            retryRecord.headers().add("message-id", messageId.getBytes());
            logger.warn("Attempting to re-send event to {}: key={}, retryCount={}, messageId={}, retryKey={}",
                    DLT_TOPIC, key, newRetryCount, messageId, retryKey);

            // Deletar a chave de retry antes do envio
            try {
                redisTemplate.delete(retryKey);
                logger.debug("Deleted retry key before send: {}", retryKey);
            } catch (Exception redisEx) {
                logger.error("Failed to delete retry key {}: error={}", retryKey, redisEx.getMessage(), redisEx);
            }

            try {
                logger.debug("Sending retry record to Kafka: topic={}, key={}, retryCount={}, messageId={}",
                        DLT_TOPIC, key, newRetryCount, messageId);
                kafkaTemplate.send(retryRecord).get(5, TimeUnit.SECONDS);
                meterRegistry.counter("dlt.redis.retries", "eventType", eventType).increment();
                logger.warn("Successfully re-sent event to {}: key={}, retryCount={}, messageId={}, retryKey={}",
                        DLT_TOPIC, key, newRetryCount, messageId, retryKey);
                acknowledgment.acknowledge();
                try {
                    Thread.sleep(500);
                    logger.debug("Waited after acknowledgment for successful retry: messageId={}", messageId);
                } catch (InterruptedException ie) {
                    logger.error("Interrupted during acknowledgment sleep: {}", ie.getMessage(), ie);
                    Thread.currentThread().interrupt();
                }
            } catch (Exception e) {
                logger.error("Failed to re-send event to {}: key={}, retryCount={}, messageId={}, retryKey={}, error={}",
                        DLT_TOPIC, key, newRetryCount, messageId, retryKey, e.getMessage(), e);
                handlePermanentError(key, eventType, "Kafka send failure: " + e.getMessage(), record, acknowledgment);
            }
        } catch (Exception e) {
            logger.error("Failed to check/set retry key in Redis: key={}, retryCount={}, messageId={}, retryKey={}, error={}",
                    key, retryCount, messageId, retryKey, e.getMessage(), e);
            try {
                redisTemplate.delete(retryKey);
                logger.debug("Deleted retry key on error: {}", retryKey);
            } catch (Exception redisEx) {
                logger.error("Failed to delete retry key {}: error={}", retryKey, redisEx.getMessage(), redisEx);
            }
            handlePermanentError(key, eventType, "Redis failure during retry: " + e.getMessage(), record, acknowledgment);
        }
    }
}
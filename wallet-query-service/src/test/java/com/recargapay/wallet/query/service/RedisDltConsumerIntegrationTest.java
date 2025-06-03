package com.recargapay.wallet.query.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.recargapay.wallet.common.event.DepositedEvent;
import com.recargapay.wallet.query.dlt.RedisDltConsumer;
import com.recargapay.wallet.query.document.WalletBalanceDocument;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@Testcontainers
@SpringBootTest
@ActiveProfiles("integration")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class RedisDltConsumerIntegrationTest {
    private static final Logger logger = LoggerFactory.getLogger(RedisDltConsumerIntegrationTest.class);

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @SpyBean
    private RedisDltConsumer redisDltConsumer;

    @SpyBean
    private CacheService cacheService;

    @Autowired
    private MeterRegistry meterRegistry;

    @BeforeEach
    void setUp() {
        String bootstrapServers = System.getProperty("spring.kafka.bootstrap-servers");
        logger.warn("Bootstrap servers: {}", bootstrapServers);
        assertNotNull(bootstrapServers, "Bootstrap servers must be set");
        assertNotNull(kafkaTemplate, "KafkaTemplate must be injected");
        assertNotNull(redisTemplate, "RedisTemplate must be injected");
        assertNotNull(objectMapper, "ObjectMapper must be injected");
        assertNotNull(redisDltConsumer, "RedisDltConsumer must be injected");
        assertNotNull(cacheService, "CacheService must be injected");
        assertNotNull(meterRegistry, "MeterRegistry must be injected");

        // Clear Redis
        try {
            redisTemplate.getConnectionFactory().getConnection().flushAll();
            redisTemplate.keys("dlt:retry:*").forEach(key -> redisTemplate.delete(key));
            logger.warn("Redis database cleared, including dlt:retry:* keys");
        } catch (Exception e) {
            logger.error("Failed to clear Redis: {}", e.getMessage(), e);
            throw new IllegalStateException("Failed to clear Redis", e);
        }

        if (bootstrapServers.isEmpty()) {
            logger.error("Bootstrap servers not set, skipping topic cleanup");
            return;
        }

        // Clear Kafka topics
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-clear-group-" + UUID.randomUUID().toString());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

        try (org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Arrays.asList("wallet-query-dlt", "wallet-query-dlt-failed"));
            logger.warn("Clearing messages from topics wallet-query-dlt and wallet-query-dlt-failed");
            long startTime = System.currentTimeMillis();
            while (System.currentTimeMillis() - startTime < 5000) { // Poll for 5 seconds
                var records = consumer.poll(Duration.ofMillis(100));
                if (records.isEmpty()) {
                    break;
                }
            }
            consumer.commitSync();
            logger.warn("Topics wallet-query-dlt and wallet-query-dlt-failed cleared of messages");
        } catch (Exception e) {
            logger.error("Error clearing messages from topics: {}", e.getMessage(), e);
        }

        // Reset mocks and metrics
        reset(redisDltConsumer, cacheService);
        meterRegistry.clear();
        logger.warn("MeterRegistry cleared");
    }

    @Test
    void testKeysExpiredDiscarded() throws Exception {
        UUID walletId = UUID.randomUUID();
        UUID transactionId = UUID.randomUUID();
        DepositedEvent event = new DepositedEvent(
                transactionId, walletId, new BigDecimal("100.00"), new BigDecimal("1100.00"),
                "TestDeposit", OffsetDateTime.now()
        );

        String balanceKey = "wallet:balance:" + walletId;
        logger.warn("Sending DepositedEvent to DLT with no cache: ID={}", walletId);

        kafkaTemplate.send("wallet-query-dlt", walletId.toString(), event).get(10, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> {
                    verify(redisDltConsumer, atLeastOnce()).consumeDlt(any(ConsumerRecord.class), any(Acknowledgment.class));
                    verify(cacheService, never()).invalidateCache(any());
                    assertEquals(1.0, meterRegistry.counter("dlt.redis.discarded", "reason", "keys_expired", "eventType", "DepositedEvent").count(),
                            "Message should be discarded due to expired keys");
                });
    }

    @Test
    void testNullEventDiscarded() throws Exception {
        UUID walletId = UUID.randomUUID();
        logger.warn("Sending null event to DLT: ID={}", walletId);

        ProducerRecord<String, Object> record = new ProducerRecord<>("wallet-query-dlt", walletId.toString(), null);
        var future = kafkaTemplate.send(record);
        var result = future.get(10, TimeUnit.SECONDS);
        logger.warn("Null event sent to topic=wallet-query-dlt, partition={}, offset={}",
                result.getRecordMetadata().partition(), result.getRecordMetadata().offset());

        await().atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofSeconds(2))
                .untilAsserted(() -> {
                    verify(redisDltConsumer, atLeastOnce()).consumeDlt(any(ConsumerRecord.class), any(Acknowledgment.class));
                    verify(cacheService, never()).invalidateCache(any());
                    assertEquals(1.0, meterRegistry.counter("dlt.redis.discarded", "reason", "null_event").count(),
                            "Null event should be discarded");
                });
    }

    @Test
    void testTransientErrorRetrySuccess() throws Exception {
        UUID walletId = UUID.randomUUID();
        UUID transactionId = UUID.randomUUID();
        DepositedEvent event = new DepositedEvent(
                transactionId, walletId, new BigDecimal("100.00"), new BigDecimal("1100.00"),
                "TestDeposit", OffsetDateTime.now()
        );

        String balanceKey = "wallet:balance:" + walletId;
        WalletBalanceDocument balance = new WalletBalanceDocument(
                walletId, UUID.randomUUID(), "testuser", "USD", new BigDecimal("1100.00"), transactionId, OffsetDateTime.now()
        );
        redisTemplate.opsForValue().set(balanceKey, balance);
        logger.warn("Set initial cache for key={}", balanceKey);

        AtomicInteger invocationCount = new AtomicInteger(0);
        doAnswer(invocation -> {
            UUID invokedWalletId = invocation.getArgument(0);
            logger.warn("invalidateCache called for walletId={}, attempt={}", invokedWalletId, invocationCount.get() + 1);
            if (invocationCount.getAndIncrement() < 2) {
                throw new RuntimeException("timeout: Simulated transient failure");
            }
            redisTemplate.delete(balanceKey);
            logger.warn("Cache invalidated successfully for key={}", balanceKey);
            return null;
        }).when(cacheService).invalidateCache(eq(walletId));

        logger.warn("Sending DepositedEvent to DLT: walletId={}", walletId);

        ProducerRecord<String, Object> record = new ProducerRecord<>("wallet-query-dlt", walletId.toString(), event);
        record.headers().add("error-message", "timeout: Simulated transient failure".getBytes());
        record.headers().add("retry-count", "0".getBytes());
        var future = kafkaTemplate.send(record);
        var result = future.get(10, TimeUnit.SECONDS);
        logger.warn("Event sent to topic=wallet-query-dlt, partition={}, offset={}",
                result.getRecordMetadata().partition(), result.getRecordMetadata().offset());

        await().atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofSeconds(2))
                .untilAsserted(() -> {
                    verify(redisDltConsumer, atLeast(3)).consumeDlt(any(ConsumerRecord.class), any(Acknowledgment.class));
                    verify(cacheService, times(3)).invalidateCache(eq(walletId));
                    assertNull(redisTemplate.opsForValue().get(balanceKey), "Cache should be invalidated after retries");
                    assertEquals(2.0, meterRegistry.counter("dlt.redis.retries", "eventType", "DepositedEvent").count(),
                            "Should have 2 retry attempts");
                    meterRegistry.getMeters().forEach(meter -> logger.warn("Metric: {}, Value: {}", meter.getId().getName(), meter.measure()));
                });
    }

    @Test
    void testTransientErrorRetryWithCacheServiceFailure() throws Exception {
        UUID walletId = UUID.randomUUID();
        UUID transactionId = UUID.randomUUID();
        DepositedEvent event = new DepositedEvent(
                transactionId, walletId, new BigDecimal("100.00"), new BigDecimal("1100.00"),
                "TestDeposit", OffsetDateTime.now()
        );

        String balanceKey = "wallet:balance:" + walletId;
        WalletBalanceDocument balance = new WalletBalanceDocument(
                walletId, UUID.randomUUID(), "testuser", "USD", new BigDecimal("1100.00"), transactionId, OffsetDateTime.now()
        );
        redisTemplate.opsForValue().set(balanceKey, balance);
        logger.warn("Set initial cache for key={}", balanceKey);

        AtomicInteger invocationCount = new AtomicInteger(0);
        doAnswer(invocation -> {
            UUID invokedWalletId = invocation.getArgument(0);
            int attempt = invocationCount.get() + 1;
            logger.warn("invalidateCache called for walletId={}, attempt={}", invokedWalletId, attempt);
            if (invocationCount.getAndIncrement() < 2) {
                throw new RuntimeException("timeout: Simulated transient failure");
            }
            throw new RuntimeException("permanent: Simulated permanent failure");
        }).when(cacheService).invalidateCache(eq(walletId));

        logger.warn("Sending DepositedEvent to DLT: walletId={}", walletId);

        ProducerRecord<String, Object> record = new ProducerRecord<>("wallet-query-dlt", walletId.toString(), event);
        record.headers().add("error-message", "timeout: Simulated transient failure".getBytes());
        record.headers().add("retry-count", "0".getBytes());
        var future = kafkaTemplate.send(record);
        var result = future.get(10, TimeUnit.SECONDS);
        logger.warn("Event sent to topic=wallet-query-dlt, partition={}, offset={}",
                result.getRecordMetadata().partition(), result.getRecordMetadata().offset());

        await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofSeconds(2))
                .untilAsserted(() -> {
                    verify(redisDltConsumer, atLeast(3)).consumeDlt(any(ConsumerRecord.class), any(Acknowledgment.class));
                    verify(cacheService, times(3)).invalidateCache(eq(walletId));
                    double retryCount = meterRegistry.counter("dlt.redis.retries", "eventType", "DepositedEvent").count();
                    logger.warn("Current dlt.redis.retries count: {}", retryCount);
                    // Log Redis retry keys
                    Set<String> retryKeys = redisTemplate.keys("dlt:retry:*");
                    logger.warn("Redis retry keys found: {}", retryKeys);
                    assertEquals(2.0, retryCount, "Should have 2 retry attempts");
                    assertEquals(1.0, meterRegistry.counter("dlt.redis.redirected", "eventType", "DepositedEvent", "reason", "permanent_error").count(),
                            "Message should be redirected to failed DLT after max retries");
                    meterRegistry.getMeters().forEach(meter -> logger.warn("Metric: {}, Value: {}", meter.getId().getName(), meter.measure()));
                });
    }
}
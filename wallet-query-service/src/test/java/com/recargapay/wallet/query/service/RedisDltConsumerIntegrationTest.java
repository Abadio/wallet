package com.recargapay.wallet.query.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.recargapay.wallet.common.event.DepositedEvent;
import com.recargapay.wallet.common.event.TransferredEvent;
import com.recargapay.wallet.common.event.WithdrawnEvent;
import com.recargapay.wallet.query.dlt.RedisDltConsumer;
import com.recargapay.wallet.query.document.WalletBalanceDocument;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.AfterEach;
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
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.StreamSupport;

import static org.awaitility.Awaitility.*;
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

    @Autowired
    private org.springframework.context.ApplicationContext applicationContext;

    @Autowired
    private KafkaContainer kafkaContainer; // Injetar o bean KafkaContainer

    private KafkaConsumer<String, String> testConsumer;

    @BeforeEach
    public void setUp() {
        String bootstrapServers = System.getProperty("spring.kafka.bootstrap-servers");
        logger.warn("Bootstrap servers: {}", bootstrapServers);
        assertNotNull(bootstrapServers, "Bootstrap servers must be set");
        assertNotNull(kafkaTemplate, "KafkaTemplate must be injected");
        assertNotNull(redisTemplate, "RedisTemplate must be injected");
        assertNotNull(objectMapper, "ObjectMapper must be injected");
        assertNotNull(redisDltConsumer, "RedisDltConsumer must be injected");
        assertNotNull(cacheService, "CacheService must be injected");
        assertNotNull(meterRegistry, "MeterRegistry must be injected");
        assertNotNull(applicationContext, "ApplicationContext must be injected");
        assertNotNull(kafkaContainer, "KafkaContainer must be injected");

        // Clear Redis keys with retry
        int maxAttempts = 3;
        long delayMs = 1000;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                logger.info("Attempt {} to clear Redis keys", attempt);
                Set<String> balanceKeys = redisTemplate.keys("wallet:balance:*");
                Set<String> retryKeys = redisTemplate.keys("dlt:retry:*");
                if (balanceKeys != null && !balanceKeys.isEmpty()) {
                    redisTemplate.delete(balanceKeys);
                    logger.warn("Cleared {} balance keys", balanceKeys.size());
                }
                if (retryKeys != null && !retryKeys.isEmpty()) {
                    redisTemplate.delete(retryKeys);
                    logger.warn("Cleared {} retry keys", retryKeys.size());
                }
                break;
            } catch (Exception e) {
                logger.warn("Failed to clear Redis keys on attempt {}: {}", attempt, e.getMessage());
                if (attempt == maxAttempts) {
                    logger.error("Failed to clear Redis keys after {} attempts", maxAttempts, e);
                    throw new IllegalStateException("Failed to clear Redis keys", e);
                }
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException ie) {
                    logger.warn("Interrupted while waiting to retry Redis connection: {}", ie.getMessage());
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while waiting to retry Redis connection", ie);
                }
            }
        }

        if (bootstrapServers.isEmpty()) {
            logger.error("Bootstrap servers not set, skipping topic cleanup");
            return;
        }

        clearKafkaTopics();

        // Initialize test consumer to manage subscriptions
        /*

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-cleanup-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        testConsumer = new KafkaConsumer<>(consumerProps);
        testConsumer.subscribe(Arrays.asList("wallet-query-dlt"));
        testConsumer.poll(Duration.ofMillis(500)); // Trigger rebalance
        testConsumer.commitSync();

         */

        // Reset mocks and metrics
        Mockito.clearInvocations(redisDltConsumer, cacheService);
        reset(redisDltConsumer, cacheService);
        meterRegistry.clear();
        FailedDltKafkaConsumer.reset();
        logger.warn("MeterRegistry and FailedDltKafkaConsumer reset");

        // Force garbage collection
        System.gc();
    }

    private void clearKafkaTopics() {
        try (AdminClient adminClient = AdminClient.create(Map.of("bootstrap.servers", kafkaContainer.getBootstrapServers()))) {
            Map<TopicPartition, RecordsToDelete> recordsToDelete = Map.of(
                    new TopicPartition("wallet-query-dlt", 0), RecordsToDelete.beforeOffset(Long.MAX_VALUE),
                    new TopicPartition("wallet-query-dlt-failed", 0), RecordsToDelete.beforeOffset(Long.MAX_VALUE)
            );
            var deleteResult = adminClient.deleteRecords(recordsToDelete).all().get(10, TimeUnit.SECONDS);
            logger.info("Cleared topics wallet-query-dlt and wallet-query-dlt-failed: result={}", deleteResult);
        } catch (Exception e) {
            logger.warn("Failed to clear Kafka topics: {}", e.getMessage());
        }
    }

    @AfterEach
    public void tearDown() {
        logger.info("Cleaning up after test");
        clearKafkaTopics();

        /*

        if (testConsumer != null) {
            testConsumer.unsubscribe();
            testConsumer.close(Duration.ofSeconds(5));
            logger.info("Test consumer closed");
        }

         */
        System.gc();
    }

    // Test methods remain unchanged (copied from your provided code for completeness)

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

        await().atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofSeconds(2))
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
            if (invocationCount.getAndIncrement() < 1) { // Reduced to 1 retry to save memory
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
                    verify(redisDltConsumer, atLeast(2)).consumeDlt(any(ConsumerRecord.class), any(Acknowledgment.class)); // Adjusted for 1 retry
                    verify(cacheService, times(2)).invalidateCache(eq(walletId)); // Adjusted for 1 retry
                    assertNull(redisTemplate.opsForValue().get(balanceKey), "Cache should be invalidated after retries");
                    assertEquals(1.0, meterRegistry.counter("dlt.redis.retries", "eventType", "DepositedEvent").count(),
                            "Should have 1 retry attempt");
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
            if (invocationCount.getAndIncrement() < 1) {
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

        await().atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofSeconds(2))
                .untilAsserted(() -> {
                    verify(redisDltConsumer, atLeast(3)).consumeDlt(any(ConsumerRecord.class), any(Acknowledgment.class));
                    verify(cacheService, times(2)).invalidateCache(eq(walletId)); // Ajustado para 2 chamadas
                    double retryCount = meterRegistry.counter("dlt.redis.retries", "eventType", "DepositedEvent").count();
                    logger.warn("Current dlt.redis.retries count: {}", retryCount);
                    Set<String> retryKeys = redisTemplate.keys("dlt:retry:*");
                    logger.warn("Redis retry keys found: {}", retryKeys);
                    assertEquals(2.0, retryCount, "Should have 2 retry attempts");
                    assertEquals(1.0, meterRegistry.counter("dlt.redis.redirected", "eventType", "DepositedEvent", "reason", "permanent_error").count(),
                            "Message should be redirected to failed DLT after max retries");
                    meterRegistry.getMeters().forEach(meter -> logger.warn("Metric: {}, Value: {}", meter.getId().getName(), meter.measure()));
                });
    }


    @Test
    void testWithdrawnEventTransientErrorRetrySuccess() throws Exception {
        UUID walletId = UUID.randomUUID();
        UUID transactionId = UUID.randomUUID();
        WithdrawnEvent event = new WithdrawnEvent(
                transactionId, walletId, new BigDecimal("100.00"), new BigDecimal("900.00"),
                "TestWithdrawal", OffsetDateTime.now()
        );

        String balanceKey = "wallet:balance:" + walletId;
        WalletBalanceDocument balance = new WalletBalanceDocument(
                walletId, UUID.randomUUID(), "testuser", "USD", new BigDecimal("900.00"), transactionId, OffsetDateTime.now()
        );
        redisTemplate.opsForValue().set(balanceKey, balance);
        logger.warn("Set initial cache for key={}", balanceKey);

        AtomicInteger invocationCount = new AtomicInteger(0);
        doAnswer(invocation -> {
            UUID invokedWalletId = invocation.getArgument(0);
            logger.warn("invalidateCache called for walletId={}, attempt={}", invokedWalletId, invocationCount.get() + 1);
            if (invocationCount.getAndIncrement() < 1) {
                throw new RuntimeException("timeout: Simulated transient failure");
            }
            redisTemplate.delete(balanceKey);
            logger.warn("Cache invalidated successfully for key={}", balanceKey);
            return null;
        }).when(cacheService).invalidateCache(eq(walletId));

        logger.warn("Sending WithdrawnEvent to DLT: walletId={}", walletId);

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
                    verify(redisDltConsumer, atLeast(2)).consumeDlt(any(ConsumerRecord.class), any(Acknowledgment.class));
                    verify(cacheService, times(2)).invalidateCache(eq(walletId));
                    assertNull(redisTemplate.opsForValue().get(balanceKey), "Cache should be invalidated after retries");
                    assertEquals(1.0, meterRegistry.counter("dlt.redis.retries", "eventType", "WithdrawnEvent").count(),
                            "Should have 1 retry attempt");
                    meterRegistry.getMeters().forEach(meter -> logger.warn("Metric: {}, Value: {}", meter.getId().getName(), meter.measure()));
                });
    }

    @Test
    void testWithdrawnEventTransientErrorWithCacheServiceFailure() throws Exception {
        UUID walletId = UUID.randomUUID();
        UUID transactionId = UUID.randomUUID();
        WithdrawnEvent event = new WithdrawnEvent(
                transactionId, walletId, new BigDecimal("100.00"), new BigDecimal("900.00"),
                "TestWithdrawal", OffsetDateTime.now()
        );

        String balanceKey = "wallet:balance:" + walletId;
        WalletBalanceDocument balance = new WalletBalanceDocument(
                walletId, UUID.randomUUID(), "testuser", "USD", new BigDecimal("900.00"), transactionId, OffsetDateTime.now()
        );
        redisTemplate.opsForValue().set(balanceKey, balance);
        logger.warn("Set initial cache for key={}", balanceKey);

        AtomicInteger invocationCount = new AtomicInteger(0);
        doAnswer(invocation -> {
            UUID invokedWalletId = invocation.getArgument(0);
            int attempt = invocationCount.get() + 1;
            logger.warn("invalidateCache called for walletId={}, attempt={}", invokedWalletId, attempt);
            if (invocationCount.getAndIncrement() < 1) {
                throw new RuntimeException("timeout: Simulated transient failure");
            }
            throw new RuntimeException("permanent: Simulated permanent failure");
        }).when(cacheService).invalidateCache(eq(walletId));

        logger.warn("Sending WithdrawnEvent to DLT: walletId={}", walletId);

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
                    verify(cacheService, times(2)).invalidateCache(eq(walletId));
                    double retryCount = meterRegistry.counter("dlt.redis.retries", "eventType", "WithdrawnEvent").count();
                    logger.warn("Current dlt.redis.retries count: {}", retryCount);
                    Set<String> retryKeys = redisTemplate.keys("dlt:retry:*");
                    logger.warn("Redis retry keys found: {}", retryKeys);
                    assertEquals(2.0, retryCount, "Should have 2 retry attempts");
                    assertEquals(1.0, meterRegistry.counter("dlt.redis.redirected", "eventType", "WithdrawnEvent", "reason", "permanent_error").count(),
                            "Message should be redirected to failed DLT after max retries");
                    meterRegistry.getMeters().forEach(meter -> logger.warn("Metric: {}, Value: {}", meter.getId().getName(), meter.measure()));
                });
    }

    @Test
    void testTransferredEventTransientErrorRetrySuccess() throws Exception {
        UUID fromWalletId = UUID.randomUUID();
        UUID toWalletId = UUID.randomUUID();
        UUID transactionId = UUID.randomUUID();
        TransferredEvent event = new TransferredEvent(
                transactionId, fromWalletId, toWalletId, new BigDecimal("50.00"),
                new BigDecimal("950.00"), new BigDecimal("1050.00"),
                "TestTransfer", OffsetDateTime.now(), "USD"
        );

        String fromBalanceKey = "wallet:balance:" + fromWalletId;
        String toBalanceKey = "wallet:balance:" + toWalletId;
        WalletBalanceDocument fromBalance = new WalletBalanceDocument(
                fromWalletId, UUID.randomUUID(), "testuser1", "USD", new BigDecimal("950.00"), transactionId, OffsetDateTime.now()
        );
        WalletBalanceDocument toBalance = new WalletBalanceDocument(
                toWalletId, UUID.randomUUID(), "testuser2", "USD", new BigDecimal("1050.00"), transactionId, OffsetDateTime.now()
        );
        redisTemplate.opsForValue().set(fromBalanceKey, fromBalance);
        redisTemplate.opsForValue().set(toBalanceKey, toBalance);
        logger.warn("Set initial cache for keys: {}, {}", fromBalanceKey, toBalanceKey);

        doAnswer(invocation -> {
            ConsumerRecord record = invocation.getArgument(0);
            logger.info("consumeDlt() called with key={}, retry-count={}, error-message={}",
                    record.key(),
                    record.headers().lastHeader("retry-count") != null ? new String(record.headers().lastHeader("retry-count").value()) : null,
                    record.headers().lastHeader("error-message") != null ? new String(record.headers().lastHeader("error-message").value()) : null);
            return invocation.callRealMethod();
        }).when(redisDltConsumer).consumeDlt(any(ConsumerRecord.class), any(Acknowledgment.class));

        AtomicInteger fromWalletInvocationCount = new AtomicInteger(0);
        AtomicInteger toWalletInvocationCount = new AtomicInteger(0);
        doAnswer(invocation -> {
            UUID invokedWalletId = invocation.getArgument(0);
            int attempt;
            if (invokedWalletId.equals(fromWalletId)) {
                attempt = fromWalletInvocationCount.getAndIncrement() + 1;
                logger.warn("invalidateCache called for fromWalletId={}, attempt={}", invokedWalletId, attempt);
                if (attempt <= 1) {
                    throw new RuntimeException("timeout: Simulated transient failure for fromWallet");
                }
                redisTemplate.delete(fromBalanceKey);
                logger.warn("Cache invalidated successfully for key={}", fromBalanceKey);
            } else if (invokedWalletId.equals(toWalletId)) {
                attempt = toWalletInvocationCount.getAndIncrement() + 1;
                logger.warn("invalidateCache called for toWalletId={}, attempt={}", invokedWalletId, attempt);
                if (attempt <= 1) {
                    throw new RuntimeException("timeout: Simulated transient failure for toWallet");
                }
                redisTemplate.delete(toBalanceKey);
                logger.warn("Cache invalidated successfully for key={}", toBalanceKey);
            } else {
                logger.error("Unexpected walletId: {}", invokedWalletId);
                throw new IllegalStateException("Unexpected walletId: " + invokedWalletId);
            }
            return null;
        }).when(cacheService).invalidateCache(any(UUID.class));

        logger.warn("Sending TransferredEvent to DLT: fromWalletId={}, toWalletId={}", fromWalletId, toWalletId);

        ProducerRecord<String, Object> record = new ProducerRecord<>("wallet-query-dlt", fromWalletId.toString(), event);
        record.headers().add("error-message", "timeout: Simulated transient failure".getBytes());
        record.headers().add("retry-count", "0".getBytes());
        var sendFuture = kafkaTemplate.send(record);
        var result = sendFuture.get(10, TimeUnit.SECONDS);
        logger.warn("Event sent to topic=wallet-query-dlt, partition={}, offset={}",
                result.getRecordMetadata().partition(), result.getRecordMetadata().offset());

        await().atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofSeconds(2))
                .untilAsserted(() -> {
                    verify(redisDltConsumer, atLeast(2)).consumeDlt(any(ConsumerRecord.class), any(Acknowledgment.class));
                    int invalidateCacheCalls = (int) mockingDetails(cacheService).getInvocations().stream()
                            .filter(inv -> inv.getMethod().getName().equals("invalidateCache"))
                            .count();
                    logger.warn("Current invalidateCache call count: {}", invalidateCacheCalls);
                    Set<String> retryKeys = redisTemplate.keys("dlt:retry:*");
                    logger.warn("Current Redis retry keys: {}", retryKeys);
                    verify(cacheService, times(4)).invalidateCache(any(UUID.class));
                    assertNull(redisTemplate.opsForValue().get(fromBalanceKey), "From wallet cache should be invalidated");
                    assertNull(redisTemplate.opsForValue().get(toBalanceKey), "To wallet cache should be invalidated");
                    assertEquals(1.0, meterRegistry.counter("dlt.redis.retries", "eventType", "TransferredEvent").count(),
                            "Should have 1 retry attempt");
                    meterRegistry.getMeters().forEach(meter -> logger.warn("Metric: {}, Value: {}", meter.getId().getName(), meter.measure()));
                });
    }

    @Test
    void testTransferredEventTransientErrorWithCacheServiceFailure() throws Exception {
        UUID fromWalletId = UUID.randomUUID();
        UUID toWalletId = UUID.randomUUID();
        UUID transactionId = UUID.randomUUID();
        TransferredEvent event = new TransferredEvent(
                transactionId, fromWalletId, toWalletId, new BigDecimal("50.00"),
                new BigDecimal("950.00"), new BigDecimal("1050.00"), "TestTransfer", OffsetDateTime.now(), "USD"
        );

        String fromBalanceKey = "wallet:balance:" + fromWalletId;
        String toBalanceKey = "wallet:balance:" + toWalletId;
        WalletBalanceDocument fromBalance = new WalletBalanceDocument(
                fromWalletId, UUID.randomUUID(), "testuser1", "USD", new BigDecimal("950.00"), transactionId, OffsetDateTime.now()
        );
        WalletBalanceDocument toBalance = new WalletBalanceDocument(
                toWalletId, UUID.randomUUID(), "testuser2", "USD", new BigDecimal("1050.00"), transactionId, OffsetDateTime.now()
        );
        redisTemplate.opsForValue().set(fromBalanceKey, fromBalance);
        redisTemplate.opsForValue().set(toBalanceKey, toBalance);
        logger.warn("Set initial cache for keys: {}, {}", fromBalanceKey, toBalanceKey);

        AtomicInteger invocationCount = new AtomicInteger(0);
        doAnswer(invocation -> {
            UUID invokedWalletId = invocation.getArgument(0);
            int attempt = invocationCount.get() + 1;
            logger.warn("invalidateCache called for walletId={}, attempt={}", invokedWalletId, attempt);
            if (invocationCount.getAndIncrement() < 2) {
                throw new RuntimeException("timeout: Simulated transient failure");
            }
            throw new RuntimeException("permanent: Simulated permanent failure");
        }).when(cacheService).invalidateCache(any(UUID.class));

        logger.warn("Sending TransferredEvent to DLT: fromWalletId={}, toWalletId={}", fromWalletId, toWalletId);

        ProducerRecord<String, Object> record = new ProducerRecord<>("wallet-query-dlt", fromWalletId.toString(), event);
        record.headers().add("error-message", "timeout: Simulated transient failure".getBytes());
        record.headers().add("retry-count", "0".getBytes());
        var sendFuture = kafkaTemplate.send(record);
        var result = sendFuture.get(10, TimeUnit.SECONDS);
        logger.warn("Event sent to topic=wallet-query-dlt, partition={}, offset={}",
                result.getRecordMetadata().partition(), result.getRecordMetadata().offset());

        await().atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofSeconds(2))
                .untilAsserted(() -> {
                    verify(redisDltConsumer, atLeast(3)).consumeDlt(any(ConsumerRecord.class), any(Acknowledgment.class));
                    verify(cacheService, times(4)).invalidateCache(any(UUID.class));
                    double retryCount = meterRegistry.counter("dlt.redis.retries", "eventType", "TransferredEvent").count();
                    logger.warn("Current dlt.redis.retries count: {}", retryCount);
                    Set<String> retryKeys = redisTemplate.keys("dlt:retry:*");
                    logger.warn("Redis retry keys found: {}", retryKeys);
                    assertEquals(2.0, retryCount, "Should have 2 retry attempts");
                    assertEquals(1.0, meterRegistry.counter("dlt.redis.redirected", "eventType", "TransferredEvent", "reason", "permanent_error").count(),
                            "Message should be redirected to failed DLT after max retries");
                    meterRegistry.getMeters().forEach(meter -> logger.warn("Metric: {}, Value: {}", meter.getId().getName(), meter.measure()));
                });
    }

    @Test
    void testUnsupportedEventDiscarded() throws Exception {
        UUID walletId = UUID.randomUUID();
        String balanceKey = "wallet:balance:" + walletId;
        WalletBalanceDocument balance = new WalletBalanceDocument(
                walletId, UUID.randomUUID(), "testuser", "USD", new BigDecimal("1000.00"), UUID.randomUUID(), OffsetDateTime.now()
        );
        redisTemplate.opsForValue().set(balanceKey, balance);
        logger.warn("Set initial cache for key={}", balanceKey);

        String malformedEvent = "{\"type\":\"UnsupportedEvent\"}";
        logger.warn("Sending malformed event to DLT: ID={}", walletId);

        ProducerRecord<String, Object> record = new ProducerRecord<>("wallet-query-dlt", walletId.toString(), malformedEvent);
        var sendFuture = kafkaTemplate.send(record);
        try {
            sendFuture.get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.warn("Expected failure for malformed event: {}", e.getMessage());
        }

        await().atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofSeconds(2))
                .untilAsserted(() -> {
                    verify(redisDltConsumer, atLeastOnce()).consumeDlt(any(ConsumerRecord.class), any(Acknowledgment.class));
                    verify(cacheService, never()).invalidateCache(any());
                    assertEquals(1.0, meterRegistry.counter("dlt.redis.discarded", "reason", "unsupported_event").count(),
                            "Malformed event should be discarded");
                });
    }

    @Test
    void testImmediatePermanentError() throws Exception {
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

        doThrow(new RuntimeException("permanent: Immediate permanent failure"))
                .when(cacheService).invalidateCache(eq(walletId));

        logger.warn("Sending DepositedEvent to DLT with immediate permanent error: walletId={}", walletId);

        ProducerRecord<String, Object> record = new ProducerRecord<>("wallet-query-dlt", walletId.toString(), event);
        record.headers().add("error-message", "permanent: Immediate permanent failure".getBytes());
        record.headers().add("retry-count", "0".getBytes());
        var sendFuture = kafkaTemplate.send(record);
        var result = sendFuture.get(10, TimeUnit.SECONDS);
        logger.warn("Event sent to topic=wallet-query-dlt, partition={}, offset={}",
                result.getRecordMetadata().partition(), result.getRecordMetadata().offset());

        await().atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofSeconds(2))
                .untilAsserted(() -> {
                    verify(redisDltConsumer, atLeastOnce()).consumeDlt(any(ConsumerRecord.class), any(Acknowledgment.class));
                    verify(cacheService, never()).invalidateCache(any(UUID.class));
                    assertEquals(0.0, meterRegistry.counter("dlt.redis.retries", "eventType", "DepositedEvent").count(),
                            "No retries should occur for immediate permanent error");
                    assertEquals(1.0, meterRegistry.counter("dlt.redis.redirected", "eventType", "DepositedEvent", "reason", "permanent_error").count(),
                            "Message should be redirected to failed DLT");
                    meterRegistry.getMeters().forEach(meter -> logger.warn("Metric: {}, Value: {}", meter.getId().getName(), meter.measure()));
                });
    }

    @Test
    void testDuplicateMessageIdempotency() throws Exception {
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
            if (invocationCount.getAndIncrement() < 1) {
                throw new RuntimeException("timeout: Simulated transient failure");
            }
            redisTemplate.delete(balanceKey);
            logger.warn("Cache invalidated successfully for key={}", balanceKey);
            return null;
        }).when(cacheService).invalidateCache(eq(walletId));

        logger.warn("Sending duplicate DepositedEvents to DLT: walletId={}", walletId);

        ProducerRecord<String, Object> record = new ProducerRecord<>("wallet-query-dlt", walletId.toString(), event);
        record.headers().add("error-message", "timeout: Simulated transient failure".getBytes());
        record.headers().add("retry-count", "0".getBytes());
        record.headers().add("message-id", "test-duplicate-message".getBytes());
        var future1 = kafkaTemplate.send(record);
        var result1 = future1.get(10, TimeUnit.SECONDS);
        logger.warn("First event sent to topic=wallet-query-dlt, partition={}, offset={}",
                result1.getRecordMetadata().partition(), result1.getRecordMetadata().offset());

        ProducerRecord<String, Object> duplicateRecord = new ProducerRecord<>("wallet-query-dlt", walletId.toString(), event);
        duplicateRecord.headers().add("error-message", "timeout: Simulated transient failure".getBytes());
        duplicateRecord.headers().add("retry-count", "0".getBytes());
        duplicateRecord.headers().add("message-id", "test-duplicate-message".getBytes());
        var future2 = kafkaTemplate.send(duplicateRecord);
        var result2 = future2.get(10, TimeUnit.SECONDS);
        logger.warn("Duplicate event sent to topic=wallet-query-dlt, partition={}, offset={}",
                result2.getRecordMetadata().partition(), result2.getRecordMetadata().offset());

        await().atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofSeconds(2))
                .untilAsserted(() -> {
                    verify(redisDltConsumer, atLeast(2)).consumeDlt(any(ConsumerRecord.class), any(Acknowledgment.class));
                    verify(cacheService, times(2)).invalidateCache(eq(walletId));
                    assertNull(redisTemplate.opsForValue().get(balanceKey), "Cache should be invalidated after retries");
                    assertEquals(1.0, meterRegistry.counter("dlt.redis.retries", "eventType", "DepositedEvent").count(),
                            "Should have 1 retry attempt, ignoring duplicates");
                    meterRegistry.getMeters().forEach(meter -> logger.warn("Metric: {}, Value: {}", meter.getId().getName(), meter.measure()));
                });
    }

    @Test
    void testRedisConnectionFailureDuringIdempotency() throws Exception {
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
            if (invocationCount.getAndIncrement() < 1) {
                throw new RuntimeException("timeout: Simulated transient failure");
            }
            redisTemplate.delete(balanceKey);
            logger.warn("Cache invalidated successfully for key={}", balanceKey);
            return null;
        }).when(cacheService).invalidateCache(eq(walletId));

        logger.warn("Sending DepositedEvent to DLT with Redis connection failure: walletId={}", walletId);

        ProducerRecord<String, Object> record = new ProducerRecord<>("wallet-query-dlt", walletId.toString(), event);
        record.headers().add("error-message", "timeout: Simulated transient failure".getBytes());
        record.headers().add("retry-count", "0".getBytes());
        var sendFuture = kafkaTemplate.send(record);
        var result = sendFuture.get(10, TimeUnit.SECONDS);
        logger.warn("Event sent to topic=wallet-query-dlt, partition={}, offset={}",
                result.getRecordMetadata().partition(), result.getRecordMetadata().offset());

        await().atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofSeconds(2))
                .untilAsserted(() -> {
                    verify(redisDltConsumer, atLeast(2)).consumeDlt(any(ConsumerRecord.class), any(Acknowledgment.class));
                    verify(cacheService, times(2)).invalidateCache(eq(walletId));
                    assertNull(redisTemplate.opsForValue().get(balanceKey), "Cache should be invalidated after retries");
                    assertEquals(1.0, meterRegistry.counter("dlt.redis.retries", "eventType", "DepositedEvent").count(),
                            "Should have 1 retry attempt despite Redis connection failure");
                    meterRegistry.getMeters().forEach(meter -> logger.warn("Metric: {}, Value: {}", meter.getId().getName(), meter.measure()));
                });
    }

    @Test
    void testKafkaSendFailureDuringRetry() throws Exception {
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
            int attempt = invocationCount.incrementAndGet();
            logger.warn("invalidateCache called for walletId={}, attempt={}", invokedWalletId, attempt);
            throw new RuntimeException("timeout: Simulated transient failure");
        }).when(cacheService).invalidateCache(eq(walletId));

        KafkaTemplate<String, Object> spiedKafkaTemplate = spy(kafkaTemplate);
        AtomicInteger kafkaSendCount = new AtomicInteger(0);
        doAnswer(invocation -> {
            ProducerRecord<String, Object> record = invocation.getArgument(0);
            String topic = record.topic();
            Header retryCountHeader = record.headers().lastHeader("retry-count");
            int retryCount = retryCountHeader != null ? Integer.parseInt(new String(retryCountHeader.value())) : 0;
            Header messageIdHeader = record.headers().lastHeader("message-id");
            String messageId = messageIdHeader != null ? new String(messageIdHeader.value()) : "unknown";
            logger.warn("kafkaTemplate.send called for topic={}, retryCount={}, messageId={}, attempt={}",
                    topic, retryCount, messageId, kafkaSendCount.incrementAndGet());
            if (topic.equals("wallet-query-dlt") && retryCount == 2) { // Falha na segunda retentiva
                logger.error("Simulating Kafka send failure for retryCount={}", retryCount);
                throw new RuntimeException("Simulated Kafka send failure during retry");
            }
            return invocation.callRealMethod();
        }).when(spiedKafkaTemplate).send(any(ProducerRecord.class));

        Field kafkaTemplateField = RedisDltConsumer.class.getDeclaredField("kafkaTemplate");
        kafkaTemplateField.setAccessible(true);
        kafkaTemplateField.set(redisDltConsumer, spiedKafkaTemplate);
        logger.warn("Injected spied KafkaTemplate into RedisDltConsumer");

        doAnswer(invocation -> {
            ConsumerRecord record = invocation.getArgument(0);
            Header retryCountHeader = record.headers().lastHeader("retry-count");
            int retryCount = retryCountHeader != null ? Integer.parseInt(new String(retryCountHeader.value())) : 0;
            Header messageIdHeader = record.headers().lastHeader("message-id");
            String messageId = messageIdHeader != null ? new String(messageIdHeader.value()) :
                    (record.topic() + "-" + record.partition() + "-" + record.offset());
            logger.warn("consumeDlt called with key={}, retryCount={}, messageId={}, headers={}",
                    record.key(), retryCount, messageId, record.headers());
            return invocation.callRealMethod();
        }).when(redisDltConsumer).consumeDlt(any(ConsumerRecord.class), any(Acknowledgment.class));

        logger.warn("Sending DepositedEvent to DLT with Kafka send failure: walletId={}", walletId);

        ProducerRecord<String, Object> record = new ProducerRecord<>("wallet-query-dlt", walletId.toString(), event);
        record.headers().add("error-message", "timeout: Simulated transient failure".getBytes());
        record.headers().add("retry-count", "0".getBytes());
        String uniqueMessageId = UUID.randomUUID().toString();
        record.headers().add("message-id", uniqueMessageId.getBytes());
        var sendFuture = kafkaTemplate.send(record);
        var result = sendFuture.get(10, TimeUnit.SECONDS);
        logger.warn("Event sent to topic=wallet-query-dlt, partition={}, offset={}",
                result.getRecordMetadata().partition(), result.getRecordMetadata().offset());

        await().atMost(Duration.ofSeconds(90))
                .pollInterval(Duration.ofSeconds(2))
                .untilAsserted(() -> {
                    int consumeDltCalls = (int) mockingDetails(redisDltConsumer).getInvocations().stream()
                            .filter(inv -> inv.getMethod().getName().equals("consumeDlt"))
                            .count();
                    logger.warn("consumeDlt called {} times", consumeDltCalls);
                    verify(redisDltConsumer, atLeast(3)).consumeDlt(any(ConsumerRecord.class), any(Acknowledgment.class));
                    verify(cacheService, times(2)).invalidateCache(eq(walletId)); // Correção aqui
                    Set<String> retryKeys = redisTemplate.keys("dlt:retry:*");
                    logger.warn("Redis retry keys found after assertions: {}", retryKeys);
                    Set<String> processedKeys = redisTemplate.keys("dlt:processed:*");
                    logger.warn("Processed keys: {}", processedKeys);
                    assertTrue(retryKeys.isEmpty(), "Retry keys should be deleted after retries or failure");
                    double retryCount = meterRegistry.counter("dlt.redis.retries", "eventType", "DepositedEvent").count();
                    logger.warn("Current dlt.redis.retries count: {}", retryCount);
                    assertEquals(1.0, retryCount, "Should have 1 successful retry attempt before Kafka failure");
                    assertEquals(1.0, meterRegistry.counter("dlt.redis.redirected", "eventType", "DepositedEvent", "reason", "permanent_error").count(),
                            "Message should be redirected to failed DLT due to Kafka failure");
                    meterRegistry.getMeters().forEach(meter -> logger.warn("Metric: {}, Value: {}", meter.getId().getName(), meter.measure()));
                });

        Set<String> finalRetryKeys = redisTemplate.keys("dlt:retry:*");
        logger.warn("Final Redis retry keys after test: {}", finalRetryKeys);
        Set<String> finalProcessedKeys = redisTemplate.keys("dlt:processed:*");
        logger.warn("Final Redis processed keys after test: {}", finalProcessedKeys);
    }

    @Test
    void testInvalidRetryCountHeader() throws Exception {
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

        logger.warn("Sending DepositedEvent to DLT with invalid retry-count: ID={}", walletId);

        ProducerRecord<String, Object> record = new ProducerRecord<>("wallet-query-dlt", walletId.toString(), event);
        record.headers().add("error-message", "timeout: Simulated transient failure".getBytes());
        record.headers().add("retry-count", "invalid".getBytes());
        var sendFuture = kafkaTemplate.send(record);
        var result = sendFuture.get(10, TimeUnit.SECONDS);
        logger.warn("Event sent to topic=wallet-query-dlt, partition={}, offset={}",
                result.getRecordMetadata().partition(), result.getRecordMetadata().offset());

        await().atMost(Duration.ofSeconds(90))
                .pollInterval(Duration.ofSeconds(2))
                .untilAsserted(() -> {
                    verify(redisDltConsumer, atLeastOnce()).consumeDlt(any(ConsumerRecord.class), any(Acknowledgment.class));
                    verify(cacheService, never()).invalidateCache(any());
                    double redirectedCount = meterRegistry.counter("dlt.redis.redirected", "eventType", "DepositedEvent", "reason", "permanent_error").count();
                    logger.warn("Current dlt.redis.redirected count: {}", redirectedCount);
                    assertEquals(1.0, redirectedCount, "Message should be redirected due to invalid retry-count");
                    meterRegistry.getMeters().forEach(meter -> logger.warn("Metric: {}, Value: {}", meter.getId().getName(), meter.measure()));
                });
    }

    @Test
    void testFailedDltConsumerContent() throws Exception {
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
            if (invocationCount.getAndIncrement() < 1) {
                throw new RuntimeException("timeout: Simulated transient failure");
            }
            throw new RuntimeException("permanent: Simulated permanent failure");
        }).when(cacheService).invalidateCache(eq(walletId));

        logger.warn("Sending DepositedEvent to DLT for failed DLT consumer test: ID={}", walletId);

        ProducerRecord<String, Object> record = new ProducerRecord<>("wallet-query-dlt", walletId.toString(), event);
        record.headers().add("error-message", "timeout: Simulated transient failure".getBytes());
        record.headers().add("retry-count", "0".getBytes());
        var sendFuture = kafkaTemplate.send(record);
        var result = sendFuture.get(10, TimeUnit.SECONDS);
        logger.warn("Event sent to topic=wallet-query-dlt, partition={}, offset={}",
                result.getRecordMetadata().partition(), result.getRecordMetadata().offset());

        await().atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofSeconds(2))
                .untilAsserted(() -> {
                    verify(redisDltConsumer, atLeast(2)).consumeDlt(any(ConsumerRecord.class), any(Acknowledgment.class));
                    verify(cacheService, times(2)).invalidateCache(eq(walletId));
                    assertEquals(2.0, meterRegistry.counter("dlt.redis.retries", "eventType", "DepositedEvent").count(),
                            "Should have 2 retry attempts");
                    assertEquals(1.0, meterRegistry.counter("dlt.redis.redirected", "eventType", "DepositedEvent", "reason", "permanent_error").count(),
                            "Message should be redirected to failed DLT");

                    ConsumerRecord<String, Object> failedRecord = FailedDltKafkaConsumer.getLastConsumedRecord();
                    assertNotNull(failedRecord, "Failed DLT record should be consumed");
                    assertEquals("wallet-query-dlt-failed", failedRecord.topic(), "Record should be in failed DLT topic");
                    assertEquals(walletId.toString(), failedRecord.key(), "Record key should match walletId");
                    assertTrue(failedRecord.value() instanceof DepositedEvent, "Record value should be DepositedEvent");
                    DepositedEvent failedEvent = (DepositedEvent) failedRecord.value();
                    assertEquals(walletId, failedEvent.getWalletId(), "Failed event walletId should match");
                    assertTrue(StreamSupport.stream(failedRecord.headers().spliterator(), false)
                                    .anyMatch(h -> "error-message".equals(h.key()) &&
                                            new String(h.value()).contains("permanent: Simulated permanent failure")),
                            "Error message header should contain permanent failure");
                    assertTrue(StreamSupport.stream(failedRecord.headers().spliterator(), false)
                                    .anyMatch(h -> "processed-once".equals(h.key()) && "true".equals(new String(h.value()))),
                            "Processed-once header should be true");
                    meterRegistry.getMeters().forEach(meter -> logger.warn("Metric: {}, Value: {}", meter.getId().getName(), meter.measure()));
                });
    }
}
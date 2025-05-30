package com.recargapay.wallet.query.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.recargapay.wallet.common.event.DepositedEvent;
import com.recargapay.wallet.common.event.TransferredEvent;
import com.recargapay.wallet.common.event.WithdrawnEvent;
import com.recargapay.wallet.query.document.DailyBalanceDocument;
import com.recargapay.wallet.query.document.TransactionHistoryDocument;
import com.recargapay.wallet.query.document.WalletBalanceDocument;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.core.env.Environment;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Integration tests for CacheInvalidationConsumer, validating event consumption and cache invalidation.
 */
@Testcontainers
@SpringBootTest
@ActiveProfiles("integration")
public class CacheInvalidationConsumerIntegrationTest {
    private static final Logger logger = LoggerFactory.getLogger(CacheInvalidationConsumerIntegrationTest.class);

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private Environment environment;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    @SpyBean
    private CacheInvalidationConsumer cacheInvalidationConsumer;

    @SpyBean
    private CacheService cacheService;

    @BeforeEach
    void setUp() {
        logger.info("Bootstrap servers: {}", environment.getProperty("spring.kafka.bootstrap-servers"));

        // Clear Redis
        redisTemplate.getConnectionFactory().getConnection().flushAll();
        logger.info("Redis database cleared");

        // Clear messages from wallet-events topic
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put("bootstrap.servers", environment.getProperty("spring.kafka.bootstrap-servers"));
        consumerProps.put("group.id", "test-clear-group-" + UUID.randomUUID());
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("auto.offset.reset", "earliest");
        consumerProps.put("enable.auto.commit", "true");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList("wallet-events"));
            logger.info("Clearing messages from topic wallet-events");
            while (true) {
                var records = consumer.poll(Duration.ofSeconds(1)).records("wallet-events");
                if (!records.iterator().hasNext()) {
                    break;
                }
            }
            consumer.commitSync();
            logger.info("Topic wallet-events cleared of messages");
        } catch (Exception e) {
            logger.warn("Error clearing messages from topic wallet-events: {}", e.getMessage());
        }
    }

    @Test
    void testConsumeDepositedEvent() throws Exception {
        // Arrange
        UUID transactionId = UUID.randomUUID();
        UUID walletId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        String date = "2025-05-29";
        DepositedEvent event = new DepositedEvent(
                transactionId,
                walletId,
                new BigDecimal("100.00"),
                new BigDecimal("1100.00"),
                "TestDeposit20250529",
                OffsetDateTime.now()
        );

        // Simulate cached data
        WalletBalanceDocument balance = new WalletBalanceDocument(
                walletId, userId, "testuser", "USD", new BigDecimal("1100.00"), transactionId, OffsetDateTime.now()
        );
        TransactionHistoryDocument historyItem = new TransactionHistoryDocument(
                transactionId, walletId, userId, "testuser", "DEPOSIT", new BigDecimal("100.00"),
                new BigDecimal("1100.00"), null, null, "TestDeposit20250529", OffsetDateTime.now()
        );
        DailyBalanceDocument dailyBalance = new DailyBalanceDocument(
                walletId, date, userId, "testuser", new BigDecimal("1100.00"), transactionId, OffsetDateTime.now()
        );

        String balanceKey = "wallet:balance:" + walletId;
        String historyKey = "wallet:history:" + walletId;
        String dailyBalanceKey = "wallet:historical_balance:" + walletId + ":" + date;

        redisTemplate.opsForValue().set(balanceKey, balance);
        redisTemplate.opsForValue().set(historyKey, List.of(historyItem));
        redisTemplate.opsForValue().set(dailyBalanceKey, dailyBalance);

        logger.info("Sending DepositedEvent: transactionId={}, walletId={}", transactionId, walletId);

        // Act
        kafkaTemplate.send("wallet-events", walletId.toString(), event).get(10, TimeUnit.SECONDS);

        // Assert
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            verify(cacheInvalidationConsumer, times(1)).consumeEvent(any(ConsumerRecord.class), any(Acknowledgment.class));
            verify(cacheService, times(1)).invalidateCache(walletId);
            assertNull(redisTemplate.opsForValue().get(balanceKey), "Balance cache should be invalidated");
            assertNull(redisTemplate.opsForValue().get(historyKey), "History cache should be invalidated");
            assertNull(redisTemplate.opsForValue().get(dailyBalanceKey), "Daily balance cache should be invalidated");
        });
    }

    @Test
    void testConsumeWithdrawnEvent() throws Exception {
        // Arrange
        UUID transactionId = UUID.randomUUID();
        UUID walletId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        String date = "2025-05-29";
        WithdrawnEvent event = new WithdrawnEvent(
                transactionId,
                walletId,
                new BigDecimal("50.00"),
                new BigDecimal("1050.00"),
                "TestWithdraw20250529",
                OffsetDateTime.now()
        );

        // Simulate cached data
        WalletBalanceDocument balance = new WalletBalanceDocument(
                walletId, userId, "testuser", "USD", new BigDecimal("1050.00"), transactionId, OffsetDateTime.now()
        );
        TransactionHistoryDocument historyItem = new TransactionHistoryDocument(
                transactionId, walletId, userId, "testuser", "WITHDRAW", new BigDecimal("50.00"),
                new BigDecimal("1050.00"), null, null, "TestWithdraw20250529", OffsetDateTime.now()
        );
        DailyBalanceDocument dailyBalance = new DailyBalanceDocument(
                walletId, date, userId, "testuser", new BigDecimal("1050.00"), transactionId, OffsetDateTime.now()
        );

        String balanceKey = "wallet:balance:" + walletId;
        String historyKey = "wallet:history:" + walletId;
        String dailyBalanceKey = "wallet:historical_balance:" + walletId + ":" + date;

        redisTemplate.opsForValue().set(balanceKey, balance);
        redisTemplate.opsForValue().set(historyKey, List.of(historyItem));
        redisTemplate.opsForValue().set(dailyBalanceKey, dailyBalance);

        logger.info("Sending WithdrawnEvent: transactionId={}, walletId={}", transactionId, walletId);

        // Act
        kafkaTemplate.send("wallet-events", walletId.toString(), event).get(10, TimeUnit.SECONDS);

        // Assert
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            verify(cacheInvalidationConsumer, times(1)).consumeEvent(any(ConsumerRecord.class), any(Acknowledgment.class));
            verify(cacheService, times(1)).invalidateCache(walletId);
            assertNull(redisTemplate.opsForValue().get(balanceKey), "Balance cache should be invalidated");
            assertNull(redisTemplate.opsForValue().get(historyKey), "History cache should be invalidated");
            assertNull(redisTemplate.opsForValue().get(dailyBalanceKey), "Daily balance cache should be invalidated");
        });
    }

    @Test
    void testConsumeTransferredEvent() throws Exception {
        // Arrange
        UUID transactionId = UUID.randomUUID();
        UUID fromWalletId = UUID.randomUUID();
        UUID toWalletId = UUID.randomUUID();
        UUID fromUserId = UUID.randomUUID();
        UUID toUserId = UUID.randomUUID();
        String date = "2025-05-29";
        TransferredEvent event = new TransferredEvent(
                transactionId,
                fromWalletId,
                toWalletId,
                new BigDecimal("200.00"),
                new BigDecimal("900.00"),
                new BigDecimal("1200.00"),
                "TestTransfer20250529",
                OffsetDateTime.now(),
                "TRANSFER_SENT"
        );

        // Simulate cached data for fromWallet
        WalletBalanceDocument fromBalance = new WalletBalanceDocument(
                fromWalletId, fromUserId, "fromuser", "USD", new BigDecimal("900.00"), transactionId, OffsetDateTime.now()
        );
        TransactionHistoryDocument fromHistoryItem = new TransactionHistoryDocument(
                transactionId, fromWalletId, fromUserId, "fromuser", "TRANSFER_SENT", new BigDecimal("200.00"),
                new BigDecimal("900.00"), toWalletId, "touser", "TestTransfer20250529", OffsetDateTime.now()
        );
        DailyBalanceDocument fromDailyBalance = new DailyBalanceDocument(
                fromWalletId, date, fromUserId, "fromuser", new BigDecimal("900.00"), transactionId, OffsetDateTime.now()
        );

        // Simulate cached data for toWallet
        WalletBalanceDocument toBalance = new WalletBalanceDocument(
                toWalletId, toUserId, "touser", "USD", new BigDecimal("1200.00"), transactionId, OffsetDateTime.now()
        );
        TransactionHistoryDocument toHistoryItem = new TransactionHistoryDocument(
                transactionId, toWalletId, toUserId, "touser", "TRANSFER_RECEIVED", new BigDecimal("200.00"),
                new BigDecimal("1200.00"), fromWalletId, "fromuser", "TestTransfer20250529", OffsetDateTime.now()
        );
        DailyBalanceDocument toDailyBalance = new DailyBalanceDocument(
                toWalletId, date, toUserId, "touser", new BigDecimal("1200.00"), transactionId, OffsetDateTime.now()
        );

        String fromBalanceKey = "wallet:balance:" + fromWalletId;
        String fromHistoryKey = "wallet:history:" + fromWalletId;
        String fromDailyBalanceKey = "wallet:historical_balance:" + fromWalletId + ":" + date;
        String toBalanceKey = "wallet:balance:" + toWalletId;
        String toHistoryKey = "wallet:history:" + toWalletId;
        String toDailyBalanceKey = "wallet:historical_balance:" + toWalletId + ":" + date;

        redisTemplate.opsForValue().set(fromBalanceKey, fromBalance);
        redisTemplate.opsForValue().set(fromHistoryKey, List.of(fromHistoryItem));
        redisTemplate.opsForValue().set(fromDailyBalanceKey, fromDailyBalance);
        redisTemplate.opsForValue().set(toBalanceKey, toBalance);
        redisTemplate.opsForValue().set(toHistoryKey, List.of(toHistoryItem));
        redisTemplate.opsForValue().set(toDailyBalanceKey, toDailyBalance);

        logger.info("Sending TransferredEvent: transactionId={}, fromWalletId={}, toWalletId={}",
                transactionId, fromWalletId, toWalletId);

        // Act
        kafkaTemplate.send("wallet-events", fromWalletId.toString(), event).get(10, TimeUnit.SECONDS);

        // Assert
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            verify(cacheInvalidationConsumer, times(1)).consumeEvent(any(ConsumerRecord.class), any(Acknowledgment.class));
            verify(cacheService, times(1)).invalidateCache(fromWalletId);
            verify(cacheService, times(1)).invalidateCache(toWalletId);
            assertNull(redisTemplate.opsForValue().get(fromBalanceKey), "From balance cache should be invalidated");
            assertNull(redisTemplate.opsForValue().get(fromHistoryKey), "From history cache should be invalidated");
            assertNull(redisTemplate.opsForValue().get(fromDailyBalanceKey), "From daily balance cache should be invalidated");
            assertNull(redisTemplate.opsForValue().get(toBalanceKey), "To balance cache should be invalidated");
            assertNull(redisTemplate.opsForValue().get(toHistoryKey), "To history cache should be invalidated");
            assertNull(redisTemplate.opsForValue().get(toDailyBalanceKey), "To daily balance cache should be invalidated");
        });
    }
}
package com.recargapay.wallet.query.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.recargapay.wallet.common.event.DepositedEvent;
import com.recargapay.wallet.common.event.TransferredEvent;
import com.recargapay.wallet.common.event.WithdrawnEvent;
import com.recargapay.wallet.query.document.DailyBalanceDocument;
import com.recargapay.wallet.query.document.TransactionHistoryDocument;
import com.recargapay.wallet.query.document.WalletBalanceDocument;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

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

        try (org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(consumerProps)) {
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
        redisTemplate.opsForList().leftPushAll(historyKey, List.of(historyItem));
        redisTemplate.opsForValue().set(dailyBalanceKey, dailyBalance);

        logger.info("Sending DepositedEvent: transactionId={}, walletId={}", transactionId, walletId);

        // Act
        kafkaTemplate.send("wallet-events", walletId.toString(), event).get(10, TimeUnit.SECONDS);

        // Assert
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            verify(cacheInvalidationConsumer, times(1)).consumeEvent(any(ConsumerRecord.class), any(Acknowledgment.class));
            verify(cacheService, times(1)).invalidateCache(walletId);
            assertNull(redisTemplate.opsForValue().get(balanceKey), "Balance cache should be invalidated");
            assertEquals(0, redisTemplate.opsForList().size(historyKey), "History cache should be invalidated");
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
                walletId, userId, "testuser", "USD", new BigDecimal("1100.00"), transactionId, OffsetDateTime.now()
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
        redisTemplate.opsForList().leftPushAll(historyKey, List.of(historyItem));
        redisTemplate.opsForValue().set(dailyBalanceKey, dailyBalance);

        logger.info("Sending WithdrawnEvent: transactionId={}, walletId={}", transactionId, walletId);

        // Act
        kafkaTemplate.send("wallet-events", walletId.toString(), event).get(10, TimeUnit.SECONDS);

        // Assert
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            verify(cacheInvalidationConsumer, times(1)).consumeEvent(any(ConsumerRecord.class), any(Acknowledgment.class));
            verify(cacheService, times(1)).invalidateCache(walletId);
            assertNull(redisTemplate.opsForValue().get(balanceKey), "Balance cache should be invalidated");
            assertEquals(0, redisTemplate.opsForList().size(historyKey), "History cache should be invalidated");
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
        String dailyBalanceKey = "wallet:historical_balance:" + toWalletId + ":" + date;

        redisTemplate.opsForValue().set(fromBalanceKey, fromBalance);
        redisTemplate.opsForList().leftPushAll(fromHistoryKey, List.of(fromHistoryItem));
        redisTemplate.opsForValue().set(fromDailyBalanceKey, fromDailyBalance);
        redisTemplate.opsForValue().set(toBalanceKey, toBalance);
        redisTemplate.opsForList().leftPushAll(toHistoryKey, List.of(toHistoryItem));
        redisTemplate.opsForValue().set(dailyBalanceKey, toDailyBalance);

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
            assertEquals(0, redisTemplate.opsForList().size(fromHistoryKey), "From history cache should be invalidated");
            assertNull(redisTemplate.opsForValue().get(fromDailyBalanceKey), "From daily balance cache should be invalidated");
            assertNull(redisTemplate.opsForValue().get(toBalanceKey), "To balance cache should be invalidated");
            assertEquals(0, redisTemplate.opsForList().size(toHistoryKey), "To history cache should be invalidated");
            assertNull(redisTemplate.opsForValue().get(dailyBalanceKey), "To daily balance cache should be invalidated");
        });
    }

    @Test
    void testConsumeDepositedEventWithFailureSendsToDLT() throws Exception {
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
        redisTemplate.opsForList().leftPushAll(historyKey, List.of(historyItem));
        redisTemplate.opsForValue().set(dailyBalanceKey, dailyBalance);

        logger.info("Initial cache state: balanceKey={}, value={}", balanceKey, redisTemplate.opsForValue().get(balanceKey));
        logger.info("Initial history cache state: historyKey={}, size={}", historyKey, redisTemplate.opsForList().size(historyKey));

        // Simulate failure in cacheService without executing the real method
        logger.info("Configuring mock to throw IllegalArgumentException for walletId={}", walletId);
        doAnswer(invocation -> {
            logger.info("Mock invoked for invalidateCache: walletId={}, balance cache state={}", invocation.getArgument(0), redisTemplate.opsForValue().get(balanceKey));
            throw new IllegalArgumentException("Simulated cache invalidation failure");
        }).when(cacheService).invalidateCache(any(UUID.class));

        logger.info("Sending DepositedEvent with simulated failure: transactionId={}, walletId={}", transactionId, walletId);

        // Act
        DltKafkaConsumer.setTestLatch(new CountDownLatch(1));
        kafkaTemplate.send("wallet-events", walletId.toString(), event).get(10, TimeUnit.SECONDS);

        // Assert
        boolean received = DltKafkaConsumer.getTestLatch().await(15, TimeUnit.SECONDS);
        if (!received) {
            logger.error("Failed to consume DLT message within 15 seconds");
        }
        assertTrue(received, "DLT message should be consumed within 15 seconds");
        ConsumerRecord<String, Object> dltRecord = DltKafkaConsumer.getLastConsumedRecord();
        logger.info("DLT record status: received={}, record={}", received, dltRecord);
        assertNotNull(dltRecord, "DLT record should not be null");
        logger.info("DLT record received: key={}, value type={}, headers={}",
                dltRecord.key(), dltRecord.value() != null ? dltRecord.value().getClass().getName() : "null", dltRecord.headers());
        assertEquals(walletId.toString(), dltRecord.key(), "DLT record key should match walletId");
        assertTrue(dltRecord.value() instanceof DepositedEvent, "DLT record value should be a DepositedEvent");
        DepositedEvent dltEvent = (DepositedEvent) dltRecord.value();
        assertEquals(event.getTransactionId(), dltEvent.getTransactionId(), "DLT event should have the same transactionId");

        logger.info("Final cache state: balanceKey={}, value={}", balanceKey, redisTemplate.opsForValue().get(balanceKey));
        logger.info("Final history cache state: historyKey={}, size={}", historyKey, redisTemplate.opsForList().size(historyKey));

        // Verify 4 invocations (1 initial + 3 retries)
        verify(cacheInvalidationConsumer, times(4)).consumeEvent(any(ConsumerRecord.class), any(Acknowledgment.class));
        verify(cacheService, times(4)).invalidateCache(any(UUID.class));

        // Assert cache contents by comparing fields
        WalletBalanceDocument cachedBalance = (WalletBalanceDocument) redisTemplate.opsForValue().get(balanceKey);
        assertNotNull(cachedBalance, "Balance cache should not be null");
        assertEquals(balance.getId(), cachedBalance.getId(), "Wallet ID should match");
        assertEquals(balance.getBalance(), cachedBalance.getBalance(), "Balance amount should match");
        assertEquals(balance.getCurrency(), cachedBalance.getCurrency(), "Currency should match");
        assertEquals(balance.getLastTransactionId(), cachedBalance.getLastTransactionId(), "Last transaction ID should match");

        List<Object> rawHistory = redisTemplate.opsForList().range(historyKey, 0, -1);
        assertNotNull(rawHistory, "History cache should not be null");
        assertEquals(1, rawHistory.size(), "History should contain one item");
        TransactionHistoryDocument cachedHistoryItem = objectMapper.convertValue(rawHistory.get(0), TransactionHistoryDocument.class);
        assertEquals(historyItem.getId(), cachedHistoryItem.getId(), "Transaction ID should match");

        DailyBalanceDocument cachedDailyBalance = (DailyBalanceDocument) redisTemplate.opsForValue().get(dailyBalanceKey);
        assertNotNull(cachedDailyBalance, "Daily balance cache should not be null");
        assertEquals(dailyBalance.getWalletId(), cachedDailyBalance.getWalletId(), "Wallet ID should match");
        assertEquals(dailyBalance.getBalance(), cachedDailyBalance.getBalance(), "Daily balance amount should match");
    }

    @Test
    void testConsumeWithdrawnEventWithFailureSendsToDLT() throws Exception {
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
                walletId, userId, "testuser", "USD", new BigDecimal("1100.00"), transactionId, OffsetDateTime.now()
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
        redisTemplate.opsForList().leftPushAll(historyKey, List.of(historyItem));
        redisTemplate.opsForValue().set(dailyBalanceKey, dailyBalance);

        logger.info("Initial cache state: balanceKey={}, value={}", balanceKey, redisTemplate.opsForValue().get(balanceKey));
        logger.info("Initial history cache state: historyKey={}, size={}", historyKey, redisTemplate.opsForList().size(historyKey));

        // Simulate failure in cacheService without executing the real method
        logger.info("Configuring mock to throw IllegalArgumentException for walletId={}", walletId);
        doAnswer(invocation -> {
            logger.info("Mock invoked for invalidateCache: walletId={}, balance cache state={}", invocation.getArgument(0), redisTemplate.opsForValue().get(balanceKey));
            throw new IllegalArgumentException("Simulated cache invalidation failure");
        }).when(cacheService).invalidateCache(any(UUID.class));

        logger.info("Sending WithdrawnEvent with simulated failure: transactionId={}, walletId={}", transactionId, walletId);

        // Act
        DltKafkaConsumer.setTestLatch(new CountDownLatch(1));
        kafkaTemplate.send("wallet-events", walletId.toString(), event).get(10, TimeUnit.SECONDS);

        // Assert
        boolean received = DltKafkaConsumer.getTestLatch().await(15, TimeUnit.SECONDS);
        if (!received) {
            logger.error("Failed to consume DLT message within 15 seconds");
        }
        assertTrue(received, "DLT message should be consumed within 15 seconds");
        ConsumerRecord<String, Object> dltRecord = DltKafkaConsumer.getLastConsumedRecord();
        logger.info("DLT record status: received={}, record={}", received, dltRecord);
        assertNotNull(dltRecord, "DLT record should not be null");
        logger.info("DLT record received: key={}, value type={}, headers={}",
                dltRecord.key(), dltRecord.value() != null ? dltRecord.value().getClass().getName() : "null", dltRecord.headers());
        assertEquals(walletId.toString(), dltRecord.key(), "DLT record key should match walletId");
        assertTrue(dltRecord.value() instanceof WithdrawnEvent, "DLT record value should be a WithdrawnEvent");
        WithdrawnEvent dltEvent = (WithdrawnEvent) dltRecord.value();
        assertEquals(event.getTransactionId(), dltEvent.getTransactionId(), "DLT event should have the same transactionId");

        logger.info("Final cache state: balanceKey={}, value={}", balanceKey, redisTemplate.opsForValue().get(balanceKey));
        logger.info("Final history cache state: historyKey={}, size={}", historyKey, redisTemplate.opsForList().size(historyKey));

        // Verify 4 invocations (1 initial + 3 retries)
        verify(cacheInvalidationConsumer, times(4)).consumeEvent(any(ConsumerRecord.class), any(Acknowledgment.class));
        verify(cacheService, times(4)).invalidateCache(any(UUID.class));

        // Assert cache contents by comparing fields
        WalletBalanceDocument cachedBalance = (WalletBalanceDocument) redisTemplate.opsForValue().get(balanceKey);
        assertNotNull(cachedBalance, "Balance cache should not be null");
        assertEquals(balance.getId(), cachedBalance.getId(), "Wallet ID should match");
        assertEquals(balance.getBalance(), cachedBalance.getBalance(), "Balance amount should match");
        assertEquals(balance.getCurrency(), cachedBalance.getCurrency(), "Currency should match");
        assertEquals(balance.getLastTransactionId(), cachedBalance.getLastTransactionId(), "Last transaction ID should match");

        List<Object> rawHistory = redisTemplate.opsForList().range(historyKey, 0, -1);
        assertNotNull(rawHistory, "History cache should not be null");
        assertEquals(1, rawHistory.size(), "History should contain one item");
        TransactionHistoryDocument cachedHistoryItem = objectMapper.convertValue(rawHistory.get(0), TransactionHistoryDocument.class);
        assertEquals(historyItem.getId(), cachedHistoryItem.getId(), "Transaction ID should match");

        DailyBalanceDocument cachedDailyBalance = (DailyBalanceDocument) redisTemplate.opsForValue().get(dailyBalanceKey);
        assertNotNull(cachedDailyBalance, "Daily balance cache should not be null");
        assertEquals(dailyBalance.getWalletId(), cachedDailyBalance.getWalletId(), "Wallet ID should match");
        assertEquals(dailyBalance.getBalance(), cachedDailyBalance.getBalance(), "Daily balance amount should match");
    }

    @Test
    void testConsumeTransferredEventWithFailureSendsToDLT() throws Exception {
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
        redisTemplate.opsForList().leftPushAll(fromHistoryKey, List.of(fromHistoryItem));
        redisTemplate.opsForValue().set(fromDailyBalanceKey, fromDailyBalance);
        redisTemplate.opsForValue().set(toBalanceKey, toBalance);
        redisTemplate.opsForList().leftPushAll(toHistoryKey, List.of(toHistoryItem));
        redisTemplate.opsForValue().set(toDailyBalanceKey, toDailyBalance);

        logger.info("Initial cache state: fromBalanceKey={}, value={}", fromBalanceKey, redisTemplate.opsForValue().get(fromBalanceKey));
        logger.info("Initial history cache state: fromHistoryKey={}, size={}", fromHistoryKey, redisTemplate.opsForList().size(fromHistoryKey));
        logger.info("Initial cache state: toBalanceKey={}, value={}", toBalanceKey, redisTemplate.opsForValue().get(toBalanceKey));
        logger.info("Initial history cache state: toHistoryKey={}, size={}", toHistoryKey, redisTemplate.opsForList().size(toHistoryKey));

        // Simulate failure in cacheService without executing the real method
        logger.info("Configuring mock to throw IllegalArgumentException for walletIds={},{}", fromWalletId, toWalletId);
        doAnswer(invocation -> {
            UUID walletId = invocation.getArgument(0);
            String key = "wallet:balance:" + walletId;
            logger.info("Mock invoked for invalidateCache: walletId={}, balance cache state={}", walletId, redisTemplate.opsForValue().get(key));
            throw new IllegalArgumentException("Simulated cache invalidation failure");
        }).when(cacheService).invalidateCache(any(UUID.class));

        logger.info("Sending TransferredEvent with simulated failure: transactionId={}, fromWalletId={}, toWalletId={}",
                transactionId, fromWalletId, toWalletId);

        // Act
        DltKafkaConsumer.setTestLatch(new CountDownLatch(1));
        kafkaTemplate.send("wallet-events", fromWalletId.toString(), event).get(10, TimeUnit.SECONDS);

        // Assert
        boolean received = DltKafkaConsumer.getTestLatch().await(15, TimeUnit.SECONDS);
        if (!received) {
            logger.error("Failed to consume DLT message within 15 seconds");
        }
        assertTrue(received, "DLT message should be consumed within 15 seconds");
        ConsumerRecord<String, Object> dltRecord = DltKafkaConsumer.getLastConsumedRecord();
        logger.info("DLT record status: received={}, record={}", received, dltRecord);
        assertNotNull(dltRecord, "DLT record should not be null");
        logger.info("DLT record received: key={}, value type={}, headers={}",
                dltRecord.key(), dltRecord.value() != null ? dltRecord.value().getClass().getName() : "null", dltRecord.headers());
        assertEquals(fromWalletId.toString(), dltRecord.key(), "DLT record key should match fromWalletId");
        assertTrue(dltRecord.value() instanceof TransferredEvent, "DLT record value should be a TransferredEvent");
        TransferredEvent dltEvent = (TransferredEvent) dltRecord.value();
        assertEquals(event.getTransactionId(), dltEvent.getTransactionId(), "DLT event should have the same transactionId");

        logger.info("Final cache state: fromBalanceKey={}, value={}", fromBalanceKey, redisTemplate.opsForValue().get(fromBalanceKey));
        logger.info("Final history cache state: fromHistoryKey={}, size={}", fromHistoryKey, redisTemplate.opsForList().size(fromHistoryKey));
        logger.info("Final cache state: toBalanceKey={}, value={}", toBalanceKey, redisTemplate.opsForValue().get(toBalanceKey));
        logger.info("Final history cache state: toHistoryKey={}, size={}", toHistoryKey, redisTemplate.opsForList().size(toHistoryKey));

        // Verify 4 invocations (1 initial + 3 retries), with 4 calls for each walletId
        verify(cacheInvalidationConsumer, times(4)).consumeEvent(any(ConsumerRecord.class), any(Acknowledgment.class));
        verify(cacheService, times(8)).invalidateCache(any(UUID.class));
        verify(cacheService, times(4)).invalidateCache(fromWalletId);
        verify(cacheService, times(4)).invalidateCache(toWalletId);

        // Assert cache contents for fromWallet
        WalletBalanceDocument cachedFromBalance = (WalletBalanceDocument) redisTemplate.opsForValue().get(fromBalanceKey);
        assertNotNull(cachedFromBalance, "From balance cache should not be null");
        assertEquals(fromBalance.getId(), cachedFromBalance.getId(), "From wallet ID should match");
        assertEquals(fromBalance.getBalance(), cachedFromBalance.getBalance(), "From balance amount should match");
        assertEquals(fromBalance.getCurrency(), cachedFromBalance.getCurrency(), "From currency should match");
        assertEquals(fromBalance.getLastTransactionId(), cachedFromBalance.getLastTransactionId(), "From last transaction ID should match");

        List<Object> rawFromHistory = redisTemplate.opsForList().range(fromHistoryKey, 0, -1);
        assertNotNull(rawFromHistory, "From history cache should not be null");
        assertEquals(1, rawFromHistory.size(), "From history should contain one item");
        TransactionHistoryDocument cachedFromHistoryItem = objectMapper.convertValue(rawFromHistory.get(0), TransactionHistoryDocument.class);
        assertEquals(fromHistoryItem.getId(), cachedFromHistoryItem.getId(), "From transaction ID should match");

        DailyBalanceDocument cachedFromDailyBalance = (DailyBalanceDocument) redisTemplate.opsForValue().get(fromDailyBalanceKey);
        assertNotNull(cachedFromDailyBalance, "From daily balance cache should not be null");
        assertEquals(fromDailyBalance.getWalletId(), cachedFromDailyBalance.getWalletId(), "From wallet ID should match");
        assertEquals(fromDailyBalance.getBalance(), cachedFromDailyBalance.getBalance(), "From daily balance amount should match");

        // Assert cache contents for toWallet
        WalletBalanceDocument cachedToBalance = (WalletBalanceDocument) redisTemplate.opsForValue().get(toBalanceKey);
        assertNotNull(cachedToBalance, "To balance cache should not be null");
        assertEquals(toBalance.getId(), cachedToBalance.getId(), "To wallet ID should match");
        assertEquals(toBalance.getBalance(), cachedToBalance.getBalance(), "To balance amount should match");
        assertEquals(toBalance.getCurrency(), cachedToBalance.getCurrency(), "To currency should match");
        assertEquals(toBalance.getLastTransactionId(), cachedToBalance.getLastTransactionId(), "To last transaction ID should match");

        List<Object> rawToHistory = redisTemplate.opsForList().range(toHistoryKey, 0, -1);
        assertNotNull(rawToHistory, "To history cache should not be null");
        assertEquals(1, rawToHistory.size(), "To history should contain one item");
        TransactionHistoryDocument cachedToHistoryItem = objectMapper.convertValue(rawToHistory.get(0), TransactionHistoryDocument.class);
        assertEquals(toHistoryItem.getId(), cachedToHistoryItem.getId(), "To transaction ID should match");

        DailyBalanceDocument cachedToDailyBalance = (DailyBalanceDocument) redisTemplate.opsForValue().get(toDailyBalanceKey);
        assertNotNull(cachedToDailyBalance, "To daily balance cache should not be null");
        assertEquals(toDailyBalance.getWalletId(), cachedToDailyBalance.getWalletId(), "To wallet ID should match");
        assertEquals(toDailyBalance.getBalance(), cachedToDailyBalance.getBalance(), "To daily balance amount should match");
    }

}
package com.recargapay.wallet.query.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.recargapay.wallet.query.config.JacksonConfig;
import com.recargapay.wallet.query.config.TestMongoConfig;
import com.recargapay.wallet.query.config.TestRedisConfig;
import com.redis.testcontainers.RedisContainer;
import com.recargapay.wallet.query.document.DailyBalanceDocument;
import com.recargapay.wallet.query.document.TransactionHistoryDocument;
import com.recargapay.wallet.query.document.WalletBalanceDocument;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(classes = {
        JacksonConfig.class,
        TestMongoConfig.class,
        TestRedisConfig.class,
        CacheService.class,
        CacheServiceIntegrationTest.TestConfig.class
})
@ActiveProfiles("integration")
@Testcontainers
class CacheServiceIntegrationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(CacheServiceIntegrationTest.class);

    private static final RedisContainer redisContainer = new RedisContainer("redis:7.0")
            .withExposedPorts(6379);

    @Configuration
    static class TestConfig {
        @Bean
        RedisContainer redisContainer() {
            return redisContainer;
        }
    }

    @BeforeAll
    static void beforeAll() {
        redisContainer.start();
        System.out.println("Redis container started on host: " + redisContainer.getHost() + ", port: " + redisContainer.getMappedPort(6379));
    }

    @Autowired
    private CacheService cacheService;

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private Environment environment;

    private UUID walletId;

    @BeforeEach
    void setUp(TestInfo testInfo) {
        // Generate a new walletId for each test
        walletId = UUID.randomUUID();

        // Clear all Redis keys
        try {
            redisTemplate.getConnectionFactory().getConnection().flushAll();
            System.out.println("Redis keys cleared");
        } catch (Exception e) {
            System.err.println("Failed to clear Redis keys: " + e.getMessage());
        }

        // Diagnostic logs
        System.out.println("Running test: " + testInfo.getDisplayName());
        System.out.println("Redis host: " + redisContainer.getHost());
        System.out.println("Redis port: " + redisContainer.getMappedPort(6379));
        System.out.println("Redis database: " + environment.getProperty("spring.data.redis.database"));
    }

    @Test
    void testCacheAndGetBalance_Success() {
        // Arrange
        WalletBalanceDocument balance = new WalletBalanceDocument(
                walletId, UUID.randomUUID(), "user1", "USD",
                new BigDecimal("100.00"), UUID.randomUUID(), OffsetDateTime.now()
        );

        // Act
        cacheService.cacheBalance(walletId, balance);
        WalletBalanceDocument result = cacheService.getBalanceFromCache(walletId);

        // Assert
        assertNotNull(result, "Cached balance should not be null");
        assertEquals(walletId, result.getId(), "Wallet ID should match");
        assertEquals("user1", result.getUsername(), "Username should match");
        assertEquals(new BigDecimal("100.00"), result.getBalance(), "Balance should match");
        assertEquals("USD", result.getCurrency(), "Currency should match");
    }

    @Test
    void testGetBalance_CacheMiss_ReturnsNull() {
        // Act
        WalletBalanceDocument result = cacheService.getBalanceFromCache(walletId);

        // Assert
        assertNull(result, "Cache miss should return null");
    }

    @Test
    void testCacheBalance_NullBalance_LogsWarning() {
        // Act
        cacheService.cacheBalance(walletId, null);

        // Assert
        WalletBalanceDocument result = cacheService.getBalanceFromCache(walletId);
        assertNull(result, "Caching null balance should result in null");
    }

    @Test
    void testCacheAndGetTransactionHistory_Success() {
        // Arrange
        TransactionHistoryDocument tx1 = new TransactionHistoryDocument(
                UUID.randomUUID(), walletId, UUID.randomUUID(), "user1", "DEPOSIT",
                new BigDecimal("50.00"), new BigDecimal("50.00"), null, null, "Deposit",
                OffsetDateTime.now().minusDays(1)
        );
        TransactionHistoryDocument tx2 = new TransactionHistoryDocument(
                UUID.randomUUID(), walletId, UUID.randomUUID(), "user1", "WITHDRAW",
                new BigDecimal("20.00"), new BigDecimal("30.00"), null, null, "Withdraw",
                OffsetDateTime.now()
        );
        List<TransactionHistoryDocument> history = Arrays.asList(tx1, tx2);

        // Act
        cacheService.cacheTransactionHistory(walletId, history);
        List<TransactionHistoryDocument> result = cacheService.getTransactionHistoryFromCache(walletId);

        // Assert
        assertNotNull(result, "Cached transaction history should not be null");
        assertEquals(2, result.size(), "History should contain 2 transactions");
        assertEquals(tx1.getId(), result.get(0).getId(), "First transaction ID should match");
        assertEquals(tx2.getId(), result.get(1).getId(), "Second transaction ID should match");
        assertEquals("DEPOSIT", result.get(0).getTransactionType(), "First transaction type should be DEPOSIT");
        assertEquals("WITHDRAW", result.get(1).getTransactionType(), "Second transaction type should be WITHDRAW");
    }

    @Test
    void testGetTransactionHistory_CacheMiss_ReturnsNull() {
        // Act
        List<TransactionHistoryDocument> result = cacheService.getTransactionHistoryFromCache(walletId);

        // Assert
        assertNull(result, "Cache miss should return null");
    }

    @Test
    void testCacheTransactionHistory_NullHistory_LogsWarning() {
        // Act
        cacheService.cacheTransactionHistory(walletId, null);

        // Assert
        List<TransactionHistoryDocument> result = cacheService.getTransactionHistoryFromCache(walletId);
        assertNull(result, "Caching null history should result in null");
    }

    @Test
    void testCacheAndGetHistoricalBalance_Success() {
        // Arrange
        String date = "2025-05-13";
        DailyBalanceDocument balance = new DailyBalanceDocument(
                walletId, date, UUID.randomUUID(), "user1",
                new BigDecimal("200.00"), UUID.randomUUID(), OffsetDateTime.now()
        );

        // Act
        cacheService.cacheHistoricalBalance(walletId, date, balance);
        DailyBalanceDocument result = cacheService.getHistoricalBalanceFromCache(walletId, date);

        // Assert
        assertNotNull(result, "Cached historical balance should not be null");
        assertEquals(walletId, result.getWalletId(), "Wallet ID should match");
        assertEquals(date, result.getDate(), "Date should match");
        assertEquals(new BigDecimal("200.00"), result.getBalance(), "Balance should match");
    }

    @Test
    void testGetHistoricalBalance_CacheMiss_ReturnsNull() {
        // Arrange
        String date = "2025-05-13";

        // Act
        DailyBalanceDocument result = cacheService.getHistoricalBalanceFromCache(walletId, date);

        // Assert
        assertNull(result, "Cache miss should return null");
    }

    @Test
    void testCacheHistoricalBalance_NullBalance_LogsWarning() {
        // Arrange
        String date = "2025-05-13";

        // Act
        cacheService.cacheHistoricalBalance(walletId, date, null);

        // Assert
        DailyBalanceDocument result = cacheService.getHistoricalBalanceFromCache(walletId, date);
        assertNull(result, "Caching null balance should result in null");
    }

    @Test
    void testInvalidateCache_RemovesAllKeys() {
        // Arrange
        WalletBalanceDocument balance = new WalletBalanceDocument(
                walletId, UUID.randomUUID(), "user1", "USD",
                new BigDecimal("100.00"), UUID.randomUUID(), OffsetDateTime.now()
        );
        TransactionHistoryDocument tx = new TransactionHistoryDocument(
                UUID.randomUUID(), walletId, UUID.randomUUID(), "user1", "DEPOSIT",
                new BigDecimal("50.00"), new BigDecimal("50.00"), null, null, "Deposit",
                OffsetDateTime.now()
        );
        String date = "2025-05-13";
        DailyBalanceDocument dailyBalance = new DailyBalanceDocument(
                walletId, date, UUID.randomUUID(), "user1",
                new BigDecimal("200.00"), UUID.randomUUID(), OffsetDateTime.now()
        );

        cacheService.cacheBalance(walletId, balance);
        cacheService.cacheTransactionHistory(walletId, Arrays.asList(tx));
        cacheService.cacheHistoricalBalance(walletId, date, dailyBalance);

        // Verify data is cached
        assertNotNull(cacheService.getBalanceFromCache(walletId), "Cached balance should not be null");
        assertNotNull(cacheService.getTransactionHistoryFromCache(walletId), "Cached transaction history should not be null");
        assertNotNull(cacheService.getHistoricalBalanceFromCache(walletId, date), "Cached historical balance should not be null");

        // Act
        cacheService.invalidateCache(walletId);

        // Assert
        assertNull(cacheService.getBalanceFromCache(walletId), "Balance cache should be invalidated");
        assertNull(cacheService.getTransactionHistoryFromCache(walletId), "Transaction history cache should be invalidated");
        assertNull(cacheService.getHistoricalBalanceFromCache(walletId, date), "Historical balance cache should be invalidated");
    }

    @Test
    void testCacheExpiration_AfterTTL_ReturnsNull() throws InterruptedException {
        // Arrange
        WalletBalanceDocument balance = new WalletBalanceDocument(
                walletId, UUID.randomUUID(), "user1", "USD",
                new BigDecimal("100.00"), UUID.randomUUID(), OffsetDateTime.now()
        );
        TransactionHistoryDocument tx = new TransactionHistoryDocument(
                UUID.randomUUID(), walletId, UUID.randomUUID(), "user1", "DEPOSIT",
                new BigDecimal("50.00"), new BigDecimal("50.00"), null, null, "Deposit",
                OffsetDateTime.now()
        );
        String date = "2025-05-13";
        DailyBalanceDocument dailyBalance = new DailyBalanceDocument(
                walletId, date, UUID.randomUUID(), "user1",
                new BigDecimal("200.00"), UUID.randomUUID(), OffsetDateTime.now()
        );

        // Act: Cache data
        cacheService.cacheBalance(walletId, balance);
        cacheService.cacheTransactionHistory(walletId, Arrays.asList(tx));
        cacheService.cacheHistoricalBalance(walletId, date, dailyBalance);

        // Verify data is cached initially
        assertNotNull(cacheService.getBalanceFromCache(walletId), "Cached balance should not be null before TTL expiration");
        assertNotNull(cacheService.getTransactionHistoryFromCache(walletId), "Cached transaction history should not be null before TTL expiration");
        assertNotNull(cacheService.getHistoricalBalanceFromCache(walletId, date), "Cached historical balance should not be null before TTL expiration");

        // Wait for TTL to expire (2 seconds + 1 second buffer)
        Thread.sleep(3000);

        // Assert: Data should be expired
        assertNull(cacheService.getBalanceFromCache(walletId), "Balance cache should be null after TTL expiration");
        assertNull(cacheService.getTransactionHistoryFromCache(walletId), "Transaction history cache should be null after TTL expiration");
        assertNull(cacheService.getHistoricalBalanceFromCache(walletId, date), "Historical balance cache should be null after TTL expiration");
    }

    @Test
    void testCacheScenario() throws Exception {
        // Use current date and previous day to make the test generic
        LocalDate today = LocalDate.now();
        LocalDate yesterday = today.minusDays(1);
        String todayStr = today.format(DateTimeFormatter.ISO_LOCAL_DATE); // e.g., "2025-05-26"
        String yesterdayStr = yesterday.format(DateTimeFormatter.ISO_LOCAL_DATE); // e.g., "2025-05-25"

        // Cache balance
        WalletBalanceDocument balance = new WalletBalanceDocument(
                walletId, UUID.randomUUID(), "user1", "USD",
                new BigDecimal("150.00"), UUID.randomUUID(), OffsetDateTime.now()
        );
        cacheService.cacheBalance(walletId, balance);
        LOGGER.info("Cached balance: {}", objectMapper.writeValueAsString(balance));

        // Cache transaction history
        TransactionHistoryDocument tx1 = new TransactionHistoryDocument(
                UUID.randomUUID(), walletId, UUID.randomUUID(), "user1", "DEPOSIT",
                new BigDecimal("200.00"), new BigDecimal("200.00"), null, null, "Initial deposit",
                OffsetDateTime.now().minusDays(1)
        );
        TransactionHistoryDocument tx2 = new TransactionHistoryDocument(
                UUID.randomUUID(), walletId, UUID.randomUUID(), "user1", "WITHDRAW",
                new BigDecimal("50.00"), new BigDecimal("150.00"), null, null, "ATM withdrawal",
                OffsetDateTime.now()
        );
        List<TransactionHistoryDocument> history = Arrays.asList(tx1, tx2);
        cacheService.cacheTransactionHistory(walletId, history);
        LOGGER.info("Cached history: {}", objectMapper.writeValueAsString(history));

        // Cache historical balances for yesterday and today
        DailyBalanceDocument balanceYesterday = new DailyBalanceDocument(
                walletId, yesterdayStr, UUID.randomUUID(), "user1",
                new BigDecimal("200.00"), UUID.randomUUID(), OffsetDateTime.now().minusDays(1)
        );
        DailyBalanceDocument balanceToday = new DailyBalanceDocument(
                walletId, todayStr, UUID.randomUUID(), "user1",
                new BigDecimal("150.00"), UUID.randomUUID(), OffsetDateTime.now()
        );
        cacheService.cacheHistoricalBalance(walletId, yesterdayStr, balanceYesterday);
        cacheService.cacheHistoricalBalance(walletId, todayStr, balanceToday);
        LOGGER.info("Cached historical balance for {}: {}", yesterdayStr, objectMapper.writeValueAsString(balanceYesterday));
        LOGGER.info("Cached historical balance for {}: {}", todayStr, objectMapper.writeValueAsString(balanceToday));

        // Invalidate cache
        cacheService.invalidateCache(walletId);

        // Verify
        assertNull(cacheService.getBalanceFromCache(walletId), "Balance should be invalidated");
        assertNull(cacheService.getTransactionHistoryFromCache(walletId), "History should be invalidated");
        assertNull(cacheService.getHistoricalBalanceFromCache(walletId, yesterdayStr), "Historical balance for yesterday should be invalidated");
        assertNull(cacheService.getHistoricalBalanceFromCache(walletId, todayStr), "Historical balance for today should be invalidated");
    }
}
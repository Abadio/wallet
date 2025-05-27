package com.recargapay.wallet.query.service;

import com.recargapay.wallet.query.config.JacksonConfig;
import com.recargapay.wallet.query.config.TestMongoConfig;
import com.recargapay.wallet.query.config.TestRedisConfig;
import com.recargapay.wallet.query.document.DailyBalanceDocument;
import com.recargapay.wallet.query.document.TransactionHistoryDocument;
import com.recargapay.wallet.query.document.WalletBalanceDocument;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.boot.test.autoconfigure.data.mongo.DataMongoTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@DataMongoTest
@EnableAutoConfiguration(exclude = {
        MongoAutoConfiguration.class,
        MongoDataAutoConfiguration.class,
        KafkaAutoConfiguration.class
})
@ActiveProfiles("integration")
@Import({TestMongoConfig.class, TestRedisConfig.class, JacksonConfig.class, WalletQueryService.class, CacheService.class})
class WalletQueryServiceIntegrationTest {

    @Autowired
    private WalletQueryService walletQueryService;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    @SpyBean
    private CacheService cacheService;

    private UUID walletId;
    private UUID userId;
    private String username;

    @BeforeEach
    void setUp() {
        // Clear MongoDB and Redis
        mongoTemplate.getDb().drop();
        redisTemplate.getConnectionFactory().getConnection().flushAll();
        System.out.println("MongoDB and Redis cleared");

        // Initialize test data
        walletId = UUID.randomUUID();
        userId = UUID.randomUUID();
        username = "testuser";
    }

    @Test
    void testGetBalance_CompleteScenario() {
        // Arrange: Insert balance
        WalletBalanceDocument balanceDoc = new WalletBalanceDocument();
        balanceDoc.setUsername(username);
        balanceDoc.setId(walletId);
        balanceDoc.setUserId(userId);
        balanceDoc.setCurrency("USD");
        balanceDoc.setBalance(new BigDecimal("1000.00"));
        balanceDoc.setLastTransactionId(UUID.randomUUID());
        balanceDoc.setUpdatedAt(OffsetDateTime.now());
        mongoTemplate.save(balanceDoc);

        // Act 1: First call (should query MongoDB)
        WalletBalanceDocument result1 = walletQueryService.getBalance(walletId);

        // Assert 1
        assertNotNull(result1);
        assertEquals(walletId, result1.getId());
        assertEquals(new BigDecimal("1000.00"), result1.getBalance());
        verify(cacheService, times(1)).getBalanceFromCache(walletId);
        verify(cacheService, times(1)).cacheBalance(eq(walletId), any(WalletBalanceDocument.class));

        // Act 2: Second call (should return from cache)
        WalletBalanceDocument result2 = walletQueryService.getBalance(walletId);

        // Assert 2
        assertNotNull(result2);
        assertEquals(walletId, result2.getId());
        assertEquals(new BigDecimal("1000.00"), result2.getBalance());
        verify(cacheService, times(2)).getBalanceFromCache(walletId); // Cumulative: 1 from Act 1 + 1 from Act 2
        verify(cacheService, times(1)).cacheBalance(eq(walletId), any(WalletBalanceDocument.class)); // Only from Act 1

        // Act 3: Invalidate cache and call again (should query MongoDB)
        cacheService.invalidateCache(walletId);
        WalletBalanceDocument result3 = walletQueryService.getBalance(walletId);

        // Assert 3
        assertNotNull(result3);
        assertEquals(walletId, result3.getId());
        assertEquals(new BigDecimal("1000.00"), result3.getBalance());
        verify(cacheService, times(3)).getBalanceFromCache(walletId); // Cumulative: 1+1+1
        verify(cacheService, times(2)).cacheBalance(eq(walletId), any(WalletBalanceDocument.class)); // Act 1 + Act 3
    }

    @Test
    void testGetTransactionHistory_CompleteScenario() {
        // Arrange: Insert transactions
        UUID transactionId1 = UUID.randomUUID();
        TransactionHistoryDocument transaction1 = new TransactionHistoryDocument();
        transaction1.setId(transactionId1);
        transaction1.setWalletId(walletId);
        transaction1.setUserId(userId);
        transaction1.setUsername(username);
        transaction1.setTransactionType("DEPOSIT");
        transaction1.setAmount(new BigDecimal("500.00"));
        transaction1.setBalanceAfter(new BigDecimal("1500.00"));
        transaction1.setRelatedWalletId(UUID.randomUUID());
        transaction1.setRelatedUsername("rel");
        transaction1.setDescription("Test transaction");
        transaction1.setCreatedAt(OffsetDateTime.now());

        UUID transactionId2 = UUID.randomUUID();
        TransactionHistoryDocument transaction2 = new TransactionHistoryDocument();
        transaction2.setId(transactionId2);
        transaction2.setWalletId(walletId);
        transaction2.setUserId(userId);
        transaction2.setUsername(username);
        transaction2.setTransactionType("WITHDRAWAL");
        transaction2.setAmount(new BigDecimal("300.00"));
        transaction2.setBalanceAfter(new BigDecimal("1200.00"));
        transaction2.setRelatedWalletId(UUID.randomUUID());
        transaction2.setRelatedUsername("rel2");
        transaction2.setDescription("Test withdrawal");
        transaction2.setCreatedAt(OffsetDateTime.now().minusHours(1));

        mongoTemplate.save(transaction1);
        mongoTemplate.save(transaction2);

        // Act 1: First call (should query MongoDB)
        List<TransactionHistoryDocument> result1 = walletQueryService.getTransactionHistory(walletId);

        // Assert 1
        assertNotNull(result1);
        assertEquals(2, result1.size());
        assertEquals(transactionId1, result1.get(0).getId());
        assertEquals(transactionId2, result1.get(1).getId());
        verify(cacheService, times(1)).getTransactionHistoryFromCache(walletId);
        verify(cacheService, times(1)).cacheTransactionHistory(eq(walletId), anyList());

        // Act 2: Second call (should return from cache)
        List<TransactionHistoryDocument> result2 = walletQueryService.getTransactionHistory(walletId);

        // Assert 2
        assertNotNull(result2);
        assertEquals(2, result2.size());
        assertEquals(transactionId1, result2.get(0).getId());
        assertEquals(transactionId2, result2.get(1).getId());
        verify(cacheService, times(2)).getTransactionHistoryFromCache(walletId); // Cumulative: 1+1
        verify(cacheService, times(1)).cacheTransactionHistory(eq(walletId), anyList()); // Only Act 1

        // Act 3: Invalidate cache and call again (should query MongoDB)
        cacheService.invalidateCache(walletId);
        List<TransactionHistoryDocument> result3 = walletQueryService.getTransactionHistory(walletId);

        // Assert 3
        assertNotNull(result3);
        assertEquals(2, result3.size());
        assertEquals(transactionId1, result3.get(0).getId());
        assertEquals(transactionId2, result3.get(1).getId());
        verify(cacheService, times(3)).getTransactionHistoryFromCache(walletId); // Cumulative: 1+1+1
        verify(cacheService, times(2)).cacheTransactionHistory(eq(walletId), anyList()); // Act 1 + Act 3
    }

    @Test
    void testGetHistoricalBalance_CompleteScenario() {
        // Arrange: Insert historical balance
        LocalDate date = LocalDate.now();
        String dateString = date.toString();
        DailyBalanceDocument dailyBalance = new DailyBalanceDocument();
        dailyBalance.setWalletId(walletId);
        dailyBalance.setDate(dateString);
        dailyBalance.setUserId(userId);
        dailyBalance.setUsername(username);
        dailyBalance.setBalance(new BigDecimal("1000.00"));
        dailyBalance.setLastTransactionId(UUID.randomUUID());
        dailyBalance.setUpdatedAt(OffsetDateTime.now());
        mongoTemplate.save(dailyBalance);

        // Act 1: First call (should query MongoDB)
        DailyBalanceDocument result1 = walletQueryService.getHistoricalBalance(walletId, date);

        // Assert 1
        assertNotNull(result1);
        assertEquals(walletId, result1.getWalletId());
        assertEquals(new BigDecimal("1000.00"), result1.getBalance());
        assertEquals(dateString, result1.getDate());
        verify(cacheService, times(1)).getHistoricalBalanceFromCache(walletId, dateString);
        verify(cacheService, times(1)).cacheHistoricalBalance(eq(walletId), eq(dateString), any(DailyBalanceDocument.class));

        // Act 2: Second call (should return from cache)
        DailyBalanceDocument result2 = walletQueryService.getHistoricalBalance(walletId, date);

        // Assert 2
        assertNotNull(result2);
        assertEquals(walletId, result2.getWalletId());
        assertEquals(new BigDecimal("1000.00"), result2.getBalance());
        assertEquals(dateString, result2.getDate());
        verify(cacheService, times(2)).getHistoricalBalanceFromCache(walletId, dateString); // Cumulative: 1+1
        verify(cacheService, times(1)).cacheHistoricalBalance(eq(walletId), eq(dateString), any(DailyBalanceDocument.class)); // Only Act 1

        // Act 3: Invalidate cache and call again (should query MongoDB)
        cacheService.invalidateCache(walletId);
        DailyBalanceDocument result3 = walletQueryService.getHistoricalBalance(walletId, date);

        // Assert 3
        assertNotNull(result3);
        assertEquals(walletId, result3.getWalletId());
        assertEquals(new BigDecimal("1000.00"), result3.getBalance());
        assertEquals(dateString, result3.getDate());
        verify(cacheService, times(3)).getHistoricalBalanceFromCache(walletId, dateString); // Cumulative: 1+1+1
        verify(cacheService, times(2)).cacheHistoricalBalance(eq(walletId), eq(dateString), any(DailyBalanceDocument.class)); // Act 1 + Act 3
    }
}
package com.recargapay.wallet.query.service;

import com.recargapay.wallet.query.document.DailyBalanceDocument;
import com.recargapay.wallet.query.document.TransactionHistoryDocument;
import com.recargapay.wallet.query.document.WalletBalanceDocument;
import com.recargapay.wallet.query.repository.DailyBalanceMongoRepository;
import com.recargapay.wallet.query.repository.TransactionHistoryMongoRepository;
import com.recargapay.wallet.query.repository.WalletBalanceMongoRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.time.LocalDate;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@ActiveProfiles("integration")
class WalletQueryServiceIntegrationTest {

    @Autowired
    private WalletBalanceMongoRepository walletBalanceRepository;

    @Autowired
    private TransactionHistoryMongoRepository transactionHistoryRepository;

    @Autowired
    private DailyBalanceMongoRepository dailyBalanceRepository;

    @Autowired
    private WalletQueryService service;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private Environment environment;

    @BeforeEach
    void setUp(TestInfo testInfo) {
        // Limpar coleções MongoDB
        mongoTemplate.getDb().drop();
        System.out.println("MongoDB collections dropped");

        // Criar índices manualmente
        mongoTemplate.getCollection("daily_balance")
                .createIndex(new org.bson.Document("walletId", 1).append("date", 1));
        mongoTemplate.getCollection("transaction_history")
                .createIndex(new org.bson.Document("walletId", 1));
        mongoTemplate.getCollection("transaction_history")
                .createIndex(new org.bson.Document("createdAt", 1));
        System.out.println("Indexes created");

        // Logs de diagnóstico
        System.out.println("Running test: " + testInfo.getDisplayName());
        System.out.println("MongoTemplate initialized: " + (mongoTemplate != null));
        System.out.println("MongoDB URI: " + environment.getProperty("spring.data.mongodb.uri"));
        System.out.println("MongoDB port: " + environment.getProperty("spring.data.mongodb.port"));
        System.out.println("MongoDB database: " + environment.getProperty("spring.data.mongodb.database"));
        System.out.println("Embedded MongoDB enabled: " + environment.getProperty("spring.data.mongodb.embedded.enabled"));
        System.out.println("MongoDB collections: " + mongoTemplate.getCollectionNames());
        System.out.println("DailyBalance indexes: " + mongoTemplate.getCollection("daily_balance").listIndexes());
        System.out.println("TransactionHistory indexes: " + mongoTemplate.getCollection("transaction_history").listIndexes());
    }

    @Test
    void testGetBalance_Success() {
        // Arrange
        UUID walletId = UUID.randomUUID();
        WalletBalanceDocument wallet = new WalletBalanceDocument(
                walletId, UUID.randomUUID(), "user1", "USD",
                new BigDecimal("100.00"), UUID.randomUUID(), OffsetDateTime.now()
        );
        walletBalanceRepository.save(wallet);

        // Act
        WalletBalanceDocument result = service.getBalance(walletId);

        // Assert
        assertNotNull(result);
        assertEquals(walletId, result.getId());
        assertEquals("user1", result.getUsername());
        assertEquals(new BigDecimal("100.00"), result.getBalance());
    }

    @Test
    void testGetBalance_WalletNotFound_ThrowsException() {
        // Arrange
        UUID walletId = UUID.randomUUID();

        // Act & Assert
        NoSuchElementException exception = assertThrows(NoSuchElementException.class,
                () -> service.getBalance(walletId));
        assertEquals("Wallet balance not found for walletId: " + walletId, exception.getMessage());
    }

    @Test
    void testGetTransactionHistory_Success() {
        // Arrange
        UUID walletId = UUID.randomUUID();
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
        transactionHistoryRepository.save(tx1);
        transactionHistoryRepository.save(tx2);

        // Act
        List<TransactionHistoryDocument> result = service.getTransactionHistory(walletId);

        // Assert
        assertEquals(2, result.size());
        assertEquals(tx2.getId(), result.get(0).getId(), "Latest transaction should be first");
        assertEquals(tx1.getId(), result.get(1).getId());
        assertEquals("WITHDRAW", result.get(0).getTransactionType());
        assertEquals("DEPOSIT", result.get(1).getTransactionType());
    }

    @Test
    void testGetTransactionHistory_EmptyList() {
        // Arrange
        UUID walletId = UUID.randomUUID();

        // Act
        List<TransactionHistoryDocument> result = service.getTransactionHistory(walletId);

        // Assert
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetHistoricalBalance_Success() {
        // Arrange
        UUID walletId = UUID.randomUUID();
        LocalDate date = LocalDate.of(2025, 5, 13);
        String dateString = date.toString();
        DailyBalanceDocument balance = new DailyBalanceDocument(
                walletId, dateString, UUID.randomUUID(), "user1",
                new BigDecimal("200.00"), UUID.randomUUID(), OffsetDateTime.now()
        );
        dailyBalanceRepository.save(balance);

        // Act
        DailyBalanceDocument result = service.getHistoricalBalance(walletId, date);

        // Assert
        assertNotNull(result);
        assertEquals(walletId, result.getWalletId());
        assertEquals(dateString, result.getDate());
        assertEquals(new BigDecimal("200.00"), result.getBalance());
    }

    @Test
    void testGetHistoricalBalance_NotFound_ThrowsException() {
        // Arrange
        UUID walletId = UUID.randomUUID();
        LocalDate date = LocalDate.of(2025, 5, 13);

        // Act & Assert
        NoSuchElementException exception = assertThrows(NoSuchElementException.class,
                () -> service.getHistoricalBalance(walletId, date));
        assertEquals("Historical balance not found for walletId: " + walletId + " and date: " + date,
                exception.getMessage());
    }
}
package com.recargapay.wallet.consumer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.recargapay.wallet.common.event.DepositedEvent;
import com.recargapay.wallet.common.event.TransferredEvent;
import com.recargapay.wallet.common.event.WithdrawnEvent;
import com.recargapay.wallet.consumer.config.*;
import com.recargapay.wallet.consumer.document.DailyBalanceDocument;
import com.recargapay.wallet.consumer.document.TransactionHistoryDocument;
import com.recargapay.wallet.consumer.document.WalletBalanceDocument;
import com.recargapay.wallet.consumer.model.User;
import com.recargapay.wallet.consumer.model.Wallet;
import com.recargapay.wallet.consumer.repository.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(
        classes = {
                TestJacksonConfig.class,
                TestKafkaConfig.class,
                TestKafkaErrorHandler.class,
                TestMongoConfig.class,
                TestJpaConfig.class,
                TestFlywayConfig.class,
                WalletEventConsumer.class,
                WalletBalanceMongoRepository.class,
                TransactionHistoryMongoRepository.class,
                DailyBalanceMongoRepository.class,
                UserRepository.class,
                WalletRepository.class
        }
)
@ActiveProfiles("test")
@EmbeddedKafka(
        partitions = 1,
        count = 1,
        controlledShutdown = false,
        topics = {"wallet-events", "wallet-events-dlq"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers",
        ports = {0},
        brokerProperties = {"auto.create.topics.enable=true"}
)
@Testcontainers
public class WalletEventConsumerIntegrationTest {
    private static final Logger logger = LoggerFactory.getLogger(WalletEventConsumerIntegrationTest.class);

    @Container
    private static final MongoDBContainer mongoDBContainer = new MongoDBContainer("mongo:6.0")
            .withExposedPorts(27017);

    @DynamicPropertySource
    static void configureMongoDB(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", mongoDBContainer::getReplicaSetUrl);
        registry.add("spring.data.mongodb.database", () -> "wallet_service");
    }

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private Environment environment;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private WalletEventConsumer walletEventConsumer;

    @Autowired
    private WalletBalanceMongoRepository walletBalanceMongoRepository;

    @Autowired
    private TransactionHistoryMongoRepository transactionHistoryMongoRepository;

    @Autowired
    private DailyBalanceMongoRepository dailyBalanceMongoRepository;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private WalletRepository walletRepository;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private DataSource dataSource;

    private UUID userId1;
    private UUID userId2;
    private UUID walletId1;
    private UUID walletId2;

    @BeforeEach
    void setUp() {
        // Limpar coleções do MongoDB
        mongoTemplate.getDb().drop();
        logger.debug("MongoDB collections dropped");

        // Log Flyway migrations
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM wallet_service.flyway_schema_history")) {
            logger.debug("Flyway migrations applied:");
            while (rs.next()) {
                logger.debug("Migration: version={}, description={}, installed_on={}",
                        rs.getString("version"), rs.getString("description"), rs.getTimestamp("installed_on"));
            }
        } catch (SQLException e) {
            logger.error("Error checking flyway_schema_history", e);
        }

        // Log tables in wallet_service
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'WALLET_SERVICE'")) {
            logger.debug("Tables in WALLET_SERVICE:");
            while (rs.next()) {
                logger.debug("Table: {}", rs.getString("TABLE_NAME"));
            }
        } catch (SQLException e) {
            logger.error("Error listing tables in WALLET_SERVICE", e);
        }

        // Log MongoDB collections
        logger.debug("MongoDB collections:");
        mongoTemplate.getCollectionNames().forEach(collection ->
                logger.debug("Collection: {}", collection));

        // Criar e salvar User 1
        userId1 = UUID.fromString("550e8400-e29b-41d4-a716-446655440001");
        User user1 = new User();
        user1.setId(userId1);
        user1.setUsername("user1");
        user1.setEmail("user1@example.com");
        user1.setCreatedAt(OffsetDateTime.now());
        user1.setUpdatedAt(OffsetDateTime.now());
        logger.debug("Saving User1: {}", user1);
        userRepository.save(user1);
        logger.debug("User1 saved: {}", userRepository.findById(userId1));

        // Criar e salvar User 2
        userId2 = UUID.fromString("550e8400-e29b-41d4-a716-446655440003");
        User user2 = new User();
        user2.setId(userId2);
        user2.setUsername("user2");
        user2.setEmail("user2@example.com");
        user2.setCreatedAt(OffsetDateTime.now());
        user2.setUpdatedAt(OffsetDateTime.now());
        logger.debug("Saving User2: {}", user2);
        userRepository.save(user2);
        logger.debug("User2 saved: {}", userRepository.findById(userId2));

        // Criar e salvar Wallet 1
        walletId1 = UUID.fromString("550e8400-e29b-41d4-a716-446655440002");
        Wallet wallet1 = new Wallet();
        wallet1.setId(walletId1);
        wallet1.setUser(user1);
        wallet1.setCurrency("USD");
        wallet1.setCreatedAt(OffsetDateTime.now());
        wallet1.setUpdatedAt(OffsetDateTime.now());
        logger.debug("Saving Wallet1: {}", wallet1);
        walletRepository.save(wallet1);
        logger.debug("Wallet1 saved: {}", walletRepository.findById(walletId1));

        // Criar e salvar Wallet 2
        walletId2 = UUID.fromString("550e8400-e29b-41d4-a716-446655440004");
        Wallet wallet2 = new Wallet();
        wallet2.setId(walletId2);
        wallet2.setUser(user2);
        wallet2.setCurrency("USD");
        wallet2.setCreatedAt(OffsetDateTime.now());
        wallet2.setUpdatedAt(OffsetDateTime.now());
        logger.debug("Saving Wallet2: {}", wallet2);
        walletRepository.save(wallet2);
        logger.debug("Wallet2 saved: {}", walletRepository.findById(walletId2));

        logger.info("Bootstrap servers: {}", environment.getProperty("spring.kafka.bootstrap-servers"));
        logger.info("Topics created: {}", embeddedKafkaBroker.getTopics());
        logger.info("MongoDB URI: {}", mongoDBContainer.getReplicaSetUrl());
        logger.debug("Setup completed: user1={}, user2={}, wallet1={}, wallet2={}", userId1, userId2, walletId1, walletId2);
    }

    @Test
    void testConsumeDepositedEvent() throws Exception {
        // Arrange
        UUID transactionId = UUID.randomUUID();
        OffsetDateTime now = OffsetDateTime.now();
        DepositedEvent event = new DepositedEvent(
                transactionId,
                walletId1,
                new BigDecimal("100.00"),
                new BigDecimal("1100.00"),
                "TestDeposit20250517",
                now
        );
        logger.info("Serializing DepositedEvent: transactionId={}, walletId={}", transactionId, walletId1);
        String json = objectMapper.writeValueAsString(event);
        logger.info("Serialized JSON: {}", json);

        // Act
        logger.info("Sending DepositedEvent to topic wallet-events with key={}", walletId1);
        kafkaTemplate.send("wallet-events", walletId1.toString(), event).get(15, TimeUnit.SECONDS);
        logger.info("DepositedEvent sent successfully");

        // Assert
        logger.info("Waiting for DepositedEvent processing and data persistence");
        String date = now.toLocalDate().format(DateTimeFormatter.ISO_LOCAL_DATE);
        await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> {
            // Log antes de buscar WalletBalance
            logger.debug("Attempting to find WalletBalanceDocument for walletId: {}", walletId1);
            WalletBalanceDocument walletBalance = walletBalanceMongoRepository.findById(walletId1).orElse(null);
            logger.debug("WalletBalanceDocument found: {}", walletBalance);
            assertNotNull(walletBalance, "WalletBalanceDocument não foi salvo");
            assertEquals(walletId1, walletBalance.getId());
            assertEquals(0, walletBalance.getBalance().compareTo(new BigDecimal("1100.00")));

            // Log antes de buscar TransactionHistory
            logger.debug("Attempting to find TransactionHistoryDocument for transactionId: {}", transactionId);
            TransactionHistoryDocument transactionHistory = transactionHistoryMongoRepository.findById(transactionId).orElse(null);
            logger.debug("TransactionHistoryDocument found: {}", transactionHistory);
            assertNotNull(transactionHistory, "TransactionHistoryDocument não foi salvo");
            assertEquals(transactionId, transactionHistory.getId());
            assertEquals("DEPOSIT", transactionHistory.getTransactionType());
            assertEquals(0, transactionHistory.getAmount().compareTo(new BigDecimal("100.00")));

            // Log antes de buscar DailyBalance
            logger.debug("Attempting to find DailyBalanceDocument for walletId: {}, date: {}", walletId1, date);
            DailyBalanceDocument dailyBalance = dailyBalanceMongoRepository
                    .findByWalletIdAndDate(walletId1, date)
                    .orElse(null);
            logger.debug("DailyBalanceDocument found: {}", dailyBalance);
            assertNotNull(dailyBalance, "DailyBalanceDocument não foi salvo");
            assertEquals(walletId1, dailyBalance.getWalletId());
            assertEquals(0, dailyBalance.getBalance().compareTo(new BigDecimal("1100.00")));
        });
    }

    @Test
    void testConsumeWithdrawnEvent() throws Exception {
        // Arrange
        UUID transactionId = UUID.randomUUID();
        OffsetDateTime now = OffsetDateTime.now();
        WithdrawnEvent event = new WithdrawnEvent(
                transactionId,
                walletId1,
                new BigDecimal("50.00"),
                new BigDecimal("950.00"),
                "TestWithdrawal20250517",
                now
        );
        logger.info("Serializing WithdrawnEvent: transactionId={}, walletId={}", transactionId, walletId1);
        String json = objectMapper.writeValueAsString(event);
        logger.info("Serialized JSON: {}", json);

        // Act
        logger.info("Sending WithdrawnEvent to topic wallet-events with key={}", walletId1);
        kafkaTemplate.send("wallet-events", walletId1.toString(), event).get(15, TimeUnit.SECONDS);
        logger.info("WithdrawnEvent sent successfully");

        // Assert
        logger.info("Waiting for WithdrawnEvent processing and data persistence");
        String date = now.toLocalDate().format(DateTimeFormatter.ISO_LOCAL_DATE);
        await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> {
            // Log antes de buscar WalletBalance
            logger.debug("Attempting to find WalletBalanceDocument for walletId: {}", walletId1);
            WalletBalanceDocument walletBalance = walletBalanceMongoRepository.findById(walletId1).orElse(null);
            logger.debug("WalletBalanceDocument found: {}", walletBalance);
            assertNotNull(walletBalance, "WalletBalanceDocument não foi salvo");
            assertEquals(walletId1, walletBalance.getId());
            assertEquals(0, walletBalance.getBalance().compareTo(new BigDecimal("950.00")));

            // Log antes de buscar TransactionHistory
            logger.debug("Attempting to find TransactionHistoryDocument for transactionId: {}", transactionId);
            TransactionHistoryDocument transactionHistory = transactionHistoryMongoRepository.findById(transactionId).orElse(null);
            logger.debug("TransactionHistoryDocument found: {}", transactionHistory);
            assertNotNull(transactionHistory, "TransactionHistoryDocument não foi salvo");
            assertEquals(transactionId, transactionHistory.getId());
            assertEquals("WITHDRAWAL", transactionHistory.getTransactionType());
            assertEquals(0, transactionHistory.getAmount().compareTo(new BigDecimal("-50.00")));

            // Log antes de buscar DailyBalance
            logger.debug("Attempting to find DailyBalanceDocument for walletId: {}, date: {}", walletId1, date);
            DailyBalanceDocument dailyBalance = dailyBalanceMongoRepository
                    .findByWalletIdAndDate(walletId1, date)
                    .orElse(null);
            logger.debug("DailyBalanceDocument found: {}", dailyBalance);
            assertNotNull(dailyBalance, "DailyBalanceDocument não foi salvo");
            assertEquals(walletId1, dailyBalance.getWalletId());
            assertEquals(0, dailyBalance.getBalance().compareTo(new BigDecimal("950.00")));
        });
    }

    @Test
    void testConsumeTransferredEvent() throws Exception {
        // Arrange
        UUID transactionId = UUID.randomUUID();
        OffsetDateTime now = OffsetDateTime.now();
        TransferredEvent event = new TransferredEvent(
                transactionId,
                walletId1,
                walletId2,
                new BigDecimal("200.00"),
                new BigDecimal("800.00"), // fromBalanceAfter
                new BigDecimal("1200.00"), // toBalanceAfter
                "TestTransfer20250517",
                now, ""
        );
        logger.info("Serializing TransferredEvent: transactionId={}, fromWalletId={}, toWalletId={}",
                transactionId, walletId1, walletId2);
        String json = objectMapper.writeValueAsString(event);
        logger.info("Serialized JSON: {}", json);

        // Act
        logger.info("Sending TransferredEvent to topic wallet-events with key={}", transactionId);
        kafkaTemplate.send("wallet-events", transactionId.toString(), event).get(15, TimeUnit.SECONDS);
        logger.info("TransferredEvent sent successfully");

        // Assert
        logger.info("Waiting for TransferredEvent processing and data persistence");
        String date = now.toLocalDate().format(DateTimeFormatter.ISO_LOCAL_DATE);
        await().atMost(60, TimeUnit.SECONDS).untilAsserted(() -> {
            // Log antes de buscar WalletBalance (fromWallet)
            logger.debug("Attempting to find WalletBalanceDocument for fromWalletId: {}", walletId1);
            WalletBalanceDocument fromWalletBalance = walletBalanceMongoRepository.findById(walletId1).orElse(null);
            logger.debug("WalletBalanceDocument (fromWallet) found: {}", fromWalletBalance);
            assertNotNull(fromWalletBalance, "WalletBalanceDocument (fromWallet) não foi salvo");
            assertEquals(walletId1, fromWalletBalance.getId());
            assertEquals(0, fromWalletBalance.getBalance().compareTo(new BigDecimal("800.00")));

            // Log antes de buscar WalletBalance (toWallet)
            logger.debug("Attempting to find WalletBalanceDocument for toWalletId: {}", walletId2);
            WalletBalanceDocument toWalletBalance = walletBalanceMongoRepository.findById(walletId2).orElse(null);
            logger.debug("WalletBalanceDocument (toWallet) found: {}", toWalletBalance);
            assertNotNull(toWalletBalance, "WalletBalanceDocument (toWallet) não foi salvo");
            assertEquals(walletId2, toWalletBalance.getId());
            assertEquals(0, toWalletBalance.getBalance().compareTo(new BigDecimal("1200.00")));

            // Log antes de buscar TransactionHistory
            logger.debug("Attempting to find TransactionHistoryDocuments for transactionId: {}", transactionId);
            List<TransactionHistoryDocument> histories = transactionHistoryMongoRepository.findAll()
                    .stream()
                    .filter(doc -> transactionId.equals(doc.getId()))
                    .collect(Collectors.toList());
            logger.debug("TransactionHistoryDocuments found: {}", histories);
            assertEquals(2, histories.size(), "Deveria haver exatamente 2 TransactionHistoryDocuments");

            TransactionHistoryDocument sentHistory = histories.stream()
                    .filter(doc -> "TRANSFER_SENT".equals(doc.getTransactionType()))
                    .findFirst()
                    .orElse(null);
            logger.debug("TransactionHistoryDocument (TRANSFER_SENT) found: {}", sentHistory);
            assertNotNull(sentHistory, "TransactionHistoryDocument (TRANSFER_SENT) não foi salvo");
            assertEquals(transactionId, sentHistory.getId());
            assertEquals("TRANSFER_SENT", sentHistory.getTransactionType());
            assertEquals(0, sentHistory.getAmount().compareTo(new BigDecimal("-200.00")));
            assertEquals(walletId2, sentHistory.getRelatedWalletId());
            assertEquals("user2", sentHistory.getRelatedUsername());

            TransactionHistoryDocument receivedHistory = histories.stream()
                    .filter(doc -> "TRANSFER_RECEIVED".equals(doc.getTransactionType()))
                    .findFirst()
                    .orElse(null);
            logger.debug("TransactionHistoryDocument (TRANSFER_RECEIVED) found: {}", receivedHistory);
            assertNotNull(receivedHistory, "TransactionHistoryDocument (TRANSFER_RECEIVED) não foi salvo");
            assertEquals(transactionId, receivedHistory.getId());
            assertEquals("TRANSFER_RECEIVED", receivedHistory.getTransactionType());
            assertEquals(0, receivedHistory.getAmount().compareTo(new BigDecimal("200.00")));
            assertEquals(walletId1, receivedHistory.getRelatedWalletId());
            assertEquals("user1", receivedHistory.getRelatedUsername());

            // Log antes de buscar DailyBalance (fromWallet)
            logger.debug("Attempting to find DailyBalanceDocument for fromWalletId: {}, date: {}", walletId1, date);
            DailyBalanceDocument fromDailyBalance = dailyBalanceMongoRepository
                    .findByWalletIdAndDate(walletId1, date)
                    .orElse(null);
            logger.debug("DailyBalanceDocument (fromWallet) found: {}", fromDailyBalance);
            assertNotNull(fromDailyBalance, "DailyBalanceDocument (fromWallet) não foi salvo");
            assertEquals(walletId1, fromDailyBalance.getWalletId());
            assertEquals(0, fromDailyBalance.getBalance().compareTo(new BigDecimal("800.00")));

            // Log antes de buscar DailyBalance (toWallet)
            logger.debug("Attempting to find DailyBalanceDocument for toWalletId: {}, date: {}", walletId2, date);
            DailyBalanceDocument toDailyBalance = dailyBalanceMongoRepository
                    .findByWalletIdAndDate(walletId2, date)
                    .orElse(null);
            logger.debug("DailyBalanceDocument (toWallet) found: {}", toDailyBalance);
            assertNotNull(toDailyBalance, "DailyBalanceDocument (toWallet) não foi salvo");
            assertEquals(walletId2, toDailyBalance.getWalletId());
            assertEquals(0, toDailyBalance.getBalance().compareTo(new BigDecimal("1200.00")));
        });
    }
}
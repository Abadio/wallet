package com.recargapay.wallet.query.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.recargapay.wallet.common.event.DepositedEvent;
import com.recargapay.wallet.common.event.TransferredEvent;
import com.recargapay.wallet.common.event.WithdrawnEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Kafka consumer functionality, validating event production and consumption.
 */
@Testcontainers
@SpringBootTest
@ActiveProfiles("integration")
public class KafkaConsumerIntegrationTest {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerIntegrationTest.class);

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private Environment environment;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @BeforeEach
    void setUp() {
        SimpleKafkaConsumer.setTestLatch(new CountDownLatch(1));
        logger.info("Spring kafka bootstrap servers property: {}", environment.getProperty("spring.kafka.bootstrap-servers"));

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
                ConsumerRecord<String, String> record = consumer.poll(Duration.ofSeconds(1)).records("wallet-events").iterator().next();
                if (record == null) {
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
    void testEventSerialization() throws Exception {
        // Test DepositedEvent serialization
        DepositedEvent depositedEvent = new DepositedEvent(
                UUID.randomUUID(),
                UUID.randomUUID(),
                new BigDecimal("100.00"),
                new BigDecimal("1100.00"),
                "TestDeposit20250530",
                OffsetDateTime.now()
        );
        String json = objectMapper.writeValueAsString(depositedEvent);
        logger.info("Serialized DepositedEvent as JSON: {}", json);
        DepositedEvent deserializedDeposited = objectMapper.readValue(json, DepositedEvent.class);
        assertEquals(depositedEvent.getTransactionId(), deserializedDeposited.getTransactionId());

        // Test WithdrawnEvent serialization
        WithdrawnEvent withdrawnEvent = new WithdrawnEvent(
                UUID.randomUUID(),
                UUID.randomUUID(),
                new BigDecimal("50.00"),
                new BigDecimal("1050.00"),
                "TestWithdraw20250530",
                OffsetDateTime.now()
        );
        json = objectMapper.writeValueAsString(withdrawnEvent);
        logger.info("Serialized WithdrawnEvent as JSON: {}", json);
        WithdrawnEvent deserializedWithdrawn = objectMapper.readValue(json, WithdrawnEvent.class);
        assertEquals(withdrawnEvent.getTransactionId(), deserializedWithdrawn.getTransactionId());

        // Test TransferredEvent serialization
        TransferredEvent transferredEvent = new TransferredEvent(
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                new BigDecimal("75.00"),
                new BigDecimal("975.00"),
                new BigDecimal("1125.00"),
                "TestTransfer20250530",
                OffsetDateTime.now(),
                "TRANSFER_SENT"
        );
        json = objectMapper.writeValueAsString(transferredEvent);
        logger.info("Serialized TransferredEvent as JSON: {}", json);
        TransferredEvent deserializedTransferred = objectMapper.readValue(json, TransferredEvent.class);
        assertEquals(transferredEvent.getTransactionId(), deserializedTransferred.getTransactionId());

        logger.info("Serialization/deserialization successful for all events");
    }

    @Test
    void testDeserializeManually() throws Exception {
        // Test DepositedEvent deserialization
        DepositedEvent depositedEvent = new DepositedEvent(
                UUID.randomUUID(),
                UUID.randomUUID(),
                new BigDecimal("100.00"),
                new BigDecimal("1100.00"),
                "TestDeserializeDeposit",
                OffsetDateTime.now()
        );
        byte[] bytes = objectMapper.writeValueAsBytes(depositedEvent);
        RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader("__TypeId__", "com.recargapay.wallet.common.event.DepositedEvent".getBytes()));
        JsonDeserializer<Object> deserializer = new JsonDeserializer<>(Object.class, objectMapper);
        deserializer.addTrustedPackages("com.recargapay.wallet.common.event");
        Object deserialized = deserializer.deserialize("wallet-events", headers, bytes);
        logger.info("Deserialized DepositedEvent type: {}", deserialized.getClass().getName());
        assertTrue(deserialized instanceof DepositedEvent, "Deserialization should result in DepositedEvent");

        // Test WithdrawnEvent deserialization
        WithdrawnEvent withdrawnEvent = new WithdrawnEvent(
                UUID.randomUUID(),
                UUID.randomUUID(),
                new BigDecimal("50.00"),
                new BigDecimal("1050.00"),
                "TestDeserializeWithdraw",
                OffsetDateTime.now()
        );
        bytes = objectMapper.writeValueAsBytes(withdrawnEvent);
        headers = new RecordHeaders();
        headers.add(new RecordHeader("__TypeId__", "com.recargapay.wallet.common.event.WithdrawnEvent".getBytes()));
        deserialized = deserializer.deserialize("wallet-events", headers, bytes);
        logger.info("Deserialized WithdrawnEvent type: {}", deserialized.getClass().getName());
        assertTrue(deserialized instanceof WithdrawnEvent, "Deserialization should result in WithdrawnEvent");

        // Test TransferredEvent deserialization
        TransferredEvent transferredEvent = new TransferredEvent(
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                new BigDecimal("75.00"),
                new BigDecimal("975.00"),
                new BigDecimal("1125.00"),
                "TestDeserializeTransfer",
                OffsetDateTime.now(),
                "TRANSFER_SENT"
        );
        bytes = objectMapper.writeValueAsBytes(transferredEvent);
        headers = new RecordHeaders();
        headers.add(new RecordHeader("__TypeId__", "com.recargapay.wallet.common.event.TransferredEvent".getBytes()));
        deserialized = deserializer.deserialize("wallet-events", headers, bytes);
        logger.info("Deserialized TransferredEvent type: {}", deserialized.getClass().getName());
        assertTrue(deserialized instanceof TransferredEvent, "Deserialization should result in TransferredEvent");
    }

    @Test
    void testConsumeDeposited() throws Exception {
        UUID transactionId = UUID.randomUUID();
        UUID walletId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        DepositedEvent event = new DepositedEvent(
                transactionId,
                walletId,
                new BigDecimal("100"),
                new BigDecimal("1100"),
                "TestDeposit2023",
                OffsetDateTime.now()
        );

        logger.info("Sending DepositedEvent to topic wallet-events with key={}", eventId.toString());
        kafkaTemplate.send("wallet-events", eventId.toString(), event).get(10, TimeUnit.SECONDS);

        boolean received = SimpleKafkaConsumer.getTestLatch().await(10, TimeUnit.SECONDS);
        if (!received) {
            logger.error("Failed to consume DepositedEvent within 10 seconds");
        }
        assertTrue(received, "DepositedEvent should be consumed within 10 seconds");
        ConsumerRecord<String, Object> record = SimpleKafkaConsumer.getLastConsumedRecord();
        assertNotNull(record, "Received record should not be null");
        logger.info("Received record value type: {}",
                record.value() != null ? record.value().getClass().getName() : "null");
        assertTrue(record.value() instanceof DepositedEvent, "Record value should be DepositedEvent");
        DepositedEvent depositedEvent = (DepositedEvent) record.value();
        assertEquals(eventId.toString(), record.key(), "Key should match");
        assertEquals(transactionId, depositedEvent.getTransactionId(), "Transaction ID should match");
        logger.info("Successfully consumed DepositedEvent: key={}, transactionId={}",
                record.key(), depositedEvent.getTransactionId());
    }

    @Test
    void testConsumeWithdrawn() throws Exception {
        UUID transactionId = UUID.randomUUID();
        UUID walletId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        WithdrawnEvent event = new WithdrawnEvent(
                transactionId,
                walletId,
                new BigDecimal("50"),
                new BigDecimal("1050"),
                "TestWithdraw2023",
                OffsetDateTime.now()
        );

        logger.info("Sending WithdrawnEvent to topic wallet-events with key={}", eventId.toString());
        kafkaTemplate.send("wallet-events", eventId.toString(), event).get(10, TimeUnit.SECONDS);

        boolean received = SimpleKafkaConsumer.getTestLatch().await(10, TimeUnit.SECONDS);
        if (!received) {
            logger.error("Failed to consume WithdrawnEvent within 10 seconds");
        }
        assertTrue(received, "WithdrawnEvent should be consumed within 10 seconds");
        ConsumerRecord<String, Object> record = SimpleKafkaConsumer.getLastConsumedRecord();
        assertNotNull(record, "Received record should not be null");
        logger.info("Received record value type: {}",
                record.value() != null ? record.value().getClass().getName() : "null");
        assertTrue(record.value() instanceof WithdrawnEvent, "Record value should be WithdrawnEvent");
        WithdrawnEvent withdrawnEvent = (WithdrawnEvent) record.value();
        assertEquals(eventId.toString(), record.key(), "Key should match");
        assertEquals(transactionId, withdrawnEvent.getTransactionId(), "Transaction ID should match");
        logger.info("Successfully consumed WithdrawnEvent: key={}, transactionId={}",
                record.key(), withdrawnEvent.getTransactionId());
    }

    @Test
    void testConsumeTransferredSent() throws Exception {
        UUID transactionId = UUID.randomUUID();
        UUID fromWalletId = UUID.randomUUID();
        UUID toWalletId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        TransferredEvent event = new TransferredEvent(
                transactionId,
                fromWalletId,
                toWalletId,
                new BigDecimal("75"),
                new BigDecimal("975"),
                new BigDecimal("1125"),
                "TestTransferSent2023",
                OffsetDateTime.now(),
                "TRANSFER_SENT"
        );

        logger.info("Sending TransferredEvent (TRANSFER_SENT) to topic wallet-events with key={}", eventId.toString());
        kafkaTemplate.send("wallet-events", eventId.toString(), event).get(10, TimeUnit.SECONDS);

        boolean received = SimpleKafkaConsumer.getTestLatch().await(10, TimeUnit.SECONDS);
        if (!received) {
            logger.error("Failed to consume TransferredEvent (TRANSFER_SENT) within 10 seconds");
        }
        assertTrue(received, "TransferredEvent (TRANSFER_SENT) should be consumed within 10 seconds");
        ConsumerRecord<String, Object> record = SimpleKafkaConsumer.getLastConsumedRecord();
        assertNotNull(record, "Received record should not be null");
        logger.info("Received record value type: {}",
                record.value() != null ? record.value().getClass().getName() : "null");
        assertTrue(record.value() instanceof TransferredEvent, "Record value should be TransferredEvent");
        TransferredEvent transferredEvent = (TransferredEvent) record.value();
        assertEquals(eventId.toString(), record.key(), "Key should match");
        assertEquals(transactionId, transferredEvent.getTransactionId(), "Transaction ID should match");
        assertEquals("TRANSFER_SENT", transferredEvent.getTransactionType(), "Transaction type should be TRANSFER_SENT");
        logger.info("Successfully consumed TransferredEvent (TRANSFER_SENT): key={}, transactionId={}",
                record.key(), transferredEvent.getTransactionId());
    }

    @Test
    void testConsumeTransferredReceived() throws Exception {
        UUID transactionId = UUID.randomUUID();
        UUID fromWalletId = UUID.randomUUID();
        UUID toWalletId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        TransferredEvent event = new TransferredEvent(
                transactionId,
                fromWalletId,
                toWalletId,
                new BigDecimal("75"),
                new BigDecimal("975"),
                new BigDecimal("1125"),
                "TestTransferReceived2023",
                OffsetDateTime.now(),
                "TRANSFER_RECEIVED"
        );

        logger.info("Sending TransferredEvent (TRANSFER_RECEIVED) to topic wallet-events with key={}", eventId.toString());
        kafkaTemplate.send("wallet-events", eventId.toString(), event).get(10, TimeUnit.SECONDS);

        boolean received = SimpleKafkaConsumer.getTestLatch().await(10, TimeUnit.SECONDS);
        if (!received) {
            logger.error("Failed to consume TransferredEvent (TRANSFER_RECEIVED) within 10 seconds");
        }
        assertTrue(received, "TransferredEvent (TRANSFER_RECEIVED) should be consumed within 10 seconds");
        ConsumerRecord<String, Object> record = SimpleKafkaConsumer.getLastConsumedRecord();
        assertNotNull(record, "Received record should not be null");
        logger.info("Received record value type: {}",
                record.value() != null ? record.value().getClass().getName() : "null");
        assertTrue(record.value() instanceof TransferredEvent, "Record value should be TransferredEvent");
        TransferredEvent transferredEvent = (TransferredEvent) record.value();
        assertEquals(eventId.toString(), record.key(), "Key should match");
        assertEquals(transactionId, transferredEvent.getTransactionId(), "Transaction ID should match");
        assertEquals("TRANSFER_RECEIVED", transferredEvent.getTransactionType(), "Transaction type should be TRANSFER_RECEIVED");
        logger.info("Successfully consumed TransferredEvent (TRANSFER_RECEIVED): key={}, transactionId={}",
                record.key(), transferredEvent.getTransactionId());
    }
}
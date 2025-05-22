package com.recargapay.wallet.consumer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.recargapay.wallet.common.event.DepositedEvent;
import com.recargapay.wallet.common.event.TransferredEvent;
import com.recargapay.wallet.common.event.WithdrawnEvent;
import com.recargapay.wallet.consumer.config.JacksonConfig;
import com.recargapay.wallet.consumer.config.TestJacksonConfig;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(classes = {TestJacksonConfig.class})
@ActiveProfiles("test")
public class EventSerializationTest {
    private static final Logger logger = LoggerFactory.getLogger(EventSerializationTest.class);

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void testDepositedEventSerializationDeserialization() {
        // Arrange
        UUID transactionId = UUID.randomUUID();
        UUID walletId = UUID.randomUUID();
        DepositedEvent event = new DepositedEvent(
                transactionId,
                walletId,
                new BigDecimal("100.00"),
                new BigDecimal("1100.00"),
                "Test Deposit",
                OffsetDateTime.now()
        );

        // Act: Serialize with JsonSerializer
        JsonSerializer<DepositedEvent> serializer = new JsonSerializer<>(objectMapper);
        serializer.setAddTypeInfo(true);
        byte[] serialized = serializer.serialize("wallet-events", event);
        logger.info("Serialized DepositedEvent: {}", new String(serialized));

        // Act: Deserialize with JsonDeserializer
        JsonDeserializer<DepositedEvent> deserializer = new JsonDeserializer<>(DepositedEvent.class, objectMapper);
        deserializer.configure(Map.of(
                JsonDeserializer.TRUSTED_PACKAGES, "com.recargapay.wallet.common.event",
                JsonDeserializer.TYPE_MAPPINGS, "DepositedEvent:com.recargapay.wallet.common.event.DepositedEvent"
        ), false);
        DepositedEvent deserialized = deserializer.deserialize("wallet-events", serialized);
        logger.info("Deserialized DepositedEvent: transactionId={}, walletId={}, createdAt={}",
                deserialized.getTransactionId(), deserialized.getWalletId(), deserialized.getCreatedAt());

        // Assert
        assertNotNull(deserialized);
        assertEquals(event.getTransactionId(), deserialized.getTransactionId());
        assertEquals(event.getWalletId(), deserialized.getWalletId());
        assertEquals(event.getAmount(), deserialized.getAmount());
        assertEquals(event.getBalanceAfter(), deserialized.getBalanceAfter());
        assertEquals(event.getDescription(), deserialized.getDescription());
        assertTrue(event.getCreatedAt().isEqual(deserialized.getCreatedAt()),
                "OffsetDateTime should represent the same instant");
    }

    @Test
    void testTransferredEventSerializationDeserialization() {
        // Arrange
        UUID transactionId = UUID.randomUUID();
        UUID fromWalletId = UUID.randomUUID();
        UUID toWalletId = UUID.randomUUID();
        TransferredEvent event = new TransferredEvent(
                transactionId,
                fromWalletId,
                toWalletId,
                new BigDecimal("50.00"),
                new BigDecimal("1050.00"),
                new BigDecimal("150.00"),
                "Test Transfer",
                OffsetDateTime.now(), ""
        );

        // Act: Serialize with JsonSerializer
        JsonSerializer<TransferredEvent> serializer = new JsonSerializer<>(objectMapper);
        serializer.setAddTypeInfo(true);
        byte[] serialized = serializer.serialize("wallet-events", event);
        logger.info("Serialized TransferredEvent: {}", new String(serialized));

        // Act: Deserialize with JsonDeserializer
        JsonDeserializer<TransferredEvent> deserializer = new JsonDeserializer<>(TransferredEvent.class, objectMapper);
        deserializer.configure(Map.of(
                JsonDeserializer.TRUSTED_PACKAGES, "com.recargapay.wallet.common.event",
                JsonDeserializer.TYPE_MAPPINGS, "TransferredEvent:com.recargapay.wallet.common.event.TransferredEvent"
        ), false);
        TransferredEvent deserialized = deserializer.deserialize("wallet-events", serialized);
        logger.info("Deserialized TransferredEvent: transactionId={}, fromWalletId={}, toWalletId={}, createdAt={}",
                deserialized.getTransactionId(), deserialized.getFromWalletId(), deserialized.getToWalletId(), deserialized.getCreatedAt());

        // Assert
        assertNotNull(deserialized);
        assertEquals(event.getTransactionId(), deserialized.getTransactionId());
        assertEquals(event.getFromWalletId(), deserialized.getFromWalletId());
        assertEquals(event.getToWalletId(), deserialized.getToWalletId());
        assertEquals(event.getAmount(), deserialized.getAmount());
        assertEquals(event.getFromBalanceAfter(), deserialized.getFromBalanceAfter());
        assertEquals(event.getToBalanceAfter(), deserialized.getToBalanceAfter());
        assertEquals(event.getDescription(), deserialized.getDescription());
        assertTrue(event.getCreatedAt().isEqual(deserialized.getCreatedAt()),
                "OffsetDateTime should represent the same instant");
    }

    @Test
    void testWithdrawnEventSerializationDeserialization() {
        // Arrange
        UUID transactionId = UUID.randomUUID();
        UUID walletId = UUID.randomUUID();
        WithdrawnEvent event = new WithdrawnEvent(
                transactionId,
                walletId,
                new BigDecimal("200.00"),
                new BigDecimal("900.00"),
                "Test Withdrawal",
                OffsetDateTime.now()
        );

        // Act: Serialize with JsonSerializer
        JsonSerializer<WithdrawnEvent> serializer = new JsonSerializer<>(objectMapper);
        serializer.setAddTypeInfo(true);
        byte[] serialized = serializer.serialize("wallet-events", event);
        logger.info("Serialized WithdrawnEvent: {}", new String(serialized));

        // Act: Deserialize with JsonDeserializer
        JsonDeserializer<WithdrawnEvent> deserializer = new JsonDeserializer<>(WithdrawnEvent.class, objectMapper);
        deserializer.configure(Map.of(
                JsonDeserializer.TRUSTED_PACKAGES, "com.recargapay.wallet.common.event",
                JsonDeserializer.TYPE_MAPPINGS, "WithdrawnEvent:com.recargapay.wallet.common.event.WithdrawnEvent"
        ), false);
        WithdrawnEvent deserialized = deserializer.deserialize("wallet-events", serialized);
        logger.info("Deserialized WithdrawnEvent: transactionId={}, walletId={}, createdAt={}",
                deserialized.getTransactionId(), deserialized.getWalletId(), deserialized.getCreatedAt());

        // Assert
        assertNotNull(deserialized);
        assertEquals(event.getTransactionId(), deserialized.getTransactionId());
        assertEquals(event.getWalletId(), deserialized.getWalletId());
        assertEquals(event.getAmount(), deserialized.getAmount());
        assertEquals(event.getBalanceAfter(), deserialized.getBalanceAfter());
        assertEquals(event.getDescription(), deserialized.getDescription());
        assertTrue(event.getCreatedAt().isEqual(deserialized.getCreatedAt()),
                "OffsetDateTime should represent the same instant");
    }
}
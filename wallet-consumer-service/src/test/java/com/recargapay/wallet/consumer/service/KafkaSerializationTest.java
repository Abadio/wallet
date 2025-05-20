package com.recargapay.wallet.consumer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.recargapay.wallet.common.event.DepositedEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(
        partitions = 1,
        count = 1,
        controlledShutdown = false,
        topics = {"wallet-events"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers",
        ports = {0},
        brokerProperties = {"auto.create.topics.enable=true"}
)
public class KafkaSerializationTest {
    private static final Logger logger = LoggerFactory.getLogger(KafkaSerializationTest.class);

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private TestConsumer testConsumer;

    @Autowired
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        testConsumer.reset();
    }

    @Test
    void testDepositedEventSerializationAndDeserialization() throws Exception {
        // Arrange
        UUID transactionId = UUID.randomUUID();
        UUID walletId = UUID.fromString("550e8400-e29b-41d4-a716-446655440002");
        DepositedEvent event = new DepositedEvent(
                transactionId,
                walletId,
                new BigDecimal("100.00"),
                new BigDecimal("1100.00"),
                "TestDeposit20250516",
                OffsetDateTime.now()
        );
        String key = walletId.toString();

        logger.info("Sending DepositedEvent: transactionId={}, walletId={}", transactionId, walletId);
        String json = objectMapper.writeValueAsString(event);
        logger.info("Serialized JSON: {}", json);

        // Act
        kafkaTemplate.send("wallet-events", key, event).get(10, TimeUnit.SECONDS);
        logger.info("Message sent to topic wallet-events with key={}", key);

        // Assert
        boolean received = testConsumer.getLatch().await(10, TimeUnit.SECONDS);
        if (!received) {
            logger.error("Timeout waiting for message consumption");
        }
        assertEquals(0, testConsumer.getLatch().getCount(), "Message was not consumed within timeout");

        ConsumerRecord<String, DepositedEvent> record = testConsumer.getReceivedRecord();
        assertNotNull(record, "No message was consumed");
        assertEquals(key, record.key(), "Received key does not match sent key");
        assertNotNull(record.value(), "Received value is null");

        DepositedEvent receivedEvent = record.value();
        assertEquals(event.getTransactionId(), receivedEvent.getTransactionId(), "Transaction ID mismatch");
        assertEquals(event.getWalletId(), receivedEvent.getWalletId(), "Wallet ID mismatch");
        assertEquals(event.getAmount(), receivedEvent.getAmount(), "Amount mismatch");
        assertEquals(event.getBalanceAfter(), receivedEvent.getBalanceAfter(), "Balance after mismatch");
        assertEquals(event.getDescription(), receivedEvent.getDescription(), "Description mismatch");
        logger.info("Successfully validated DepositedEvent serialization and deserialization");
    }

    @Configuration
    @EnableKafka
    static class TestConfig {
        @Bean
        public ProducerFactory<String, Object> producerFactory(EmbeddedKafkaBroker embeddedKafkaBroker) {
            Map<String, Object> configProps = new HashMap<>();
            configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString());
            configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
            // Removido TYPE_MAPPINGS para usar nome completo da classe

            return new DefaultKafkaProducerFactory<>(configProps);
        }

        @Bean
        public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
            return new KafkaTemplate<>(producerFactory);
        }

        @Bean
        public ConsumerFactory<String, Object> consumerFactory(EmbeddedKafkaBroker embeddedKafkaBroker) {
            Map<String, Object> props = new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString());
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-serialization-group");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
            props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, org.springframework.kafka.support.serializer.JsonDeserializer.class.getName());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
            props.put(org.springframework.kafka.support.serializer.JsonDeserializer.TRUSTED_PACKAGES, "com.recargapay.wallet.common.event");

            return new DefaultKafkaConsumerFactory<>(props);
        }

        @Bean
        public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
                ConsumerFactory<String, Object> consumerFactory) {
            ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(consumerFactory);
            factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
            factory.setConcurrency(1);
            return factory;
        }

        @Bean
        public TestConsumer testConsumer() {
            return new TestConsumer();
        }

        @Bean
        public ObjectMapper objectMapper() {
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
            return mapper;
        }
    }

    static class TestConsumer {
        private final Logger logger = LoggerFactory.getLogger(TestConsumer.class);
        private CountDownLatch latch = new CountDownLatch(1);
        private ConsumerRecord<String, DepositedEvent> receivedRecord;

        @KafkaListener(
                topics = "wallet-events",
                groupId = "test-serialization-group",
                containerFactory = "kafkaListenerContainerFactory"
        )
        public void consume(ConsumerRecord<String, DepositedEvent> record) {
            logger.info("Consumed message: key={}, value={}", record.key(), record.value());
            this.receivedRecord = record;
            latch.countDown();
        }

        public void reset() {
            latch = new CountDownLatch(1);
            receivedRecord = null;
        }

        public CountDownLatch getLatch() {
            return latch;
        }

        public ConsumerRecord<String, DepositedEvent> getReceivedRecord() {
            return receivedRecord;
        }
    }
}
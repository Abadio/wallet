package com.recargapay.wallet.consumer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.recargapay.wallet.common.event.DepositedEvent;
import com.recargapay.wallet.consumer.config.JacksonConfig;
import com.recargapay.wallet.consumer.config.KafkaErrorHandler;
import com.recargapay.wallet.consumer.repository.DailyBalanceMongoRepository;
import com.recargapay.wallet.consumer.repository.TransactionHistoryMongoRepository;
import com.recargapay.wallet.consumer.repository.UserRepository;
import com.recargapay.wallet.consumer.repository.WalletBalanceMongoRepository;
import com.recargapay.wallet.consumer.repository.WalletRepository;
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
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest(
        classes = {
                JacksonConfig.class,
                KafkaErrorHandler.class,
                WalletEventConsumer.class
        }
)
@ActiveProfiles("test")
@EnableAutoConfiguration(exclude = {MongoAutoConfiguration.class, MongoDataAutoConfiguration.class})
@EmbeddedKafka(
        partitions = 1,
        count = 1,
        controlledShutdown = false,
        topics = {"wallet-events", "wallet-events-dlq"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers",
        ports = {0},
        brokerProperties = {"auto.create.topics.enable=true"}
)
public class WalletEventConsumerIntegrationSimpleTest {
    private static final Logger logger = LoggerFactory.getLogger(WalletEventConsumerIntegrationSimpleTest.class);

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private Environment environment;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @SpyBean
    private WalletEventConsumer walletEventConsumer;

    @Autowired
    @Qualifier("kafkaErrorHandler")
    private CommonErrorHandler kafkaErrorHandler;

    @MockBean
    private WalletBalanceMongoRepository walletBalanceMongoRepository;

    @MockBean
    private TransactionHistoryMongoRepository transactionHistoryMongoRepository;

    @MockBean
    private DailyBalanceMongoRepository dailyBalanceMongoRepository;

    @MockBean
    private UserRepository userRepository;

    @MockBean
    private WalletRepository walletRepository;

    @MockBean
    private MongoTemplate mongoTemplate;

    @BeforeEach
    void setUp() {
        logger.info("Bootstrap servers: {}", environment.getProperty("spring.kafka.bootstrap-servers"));
        logger.info("Topics created: {}", embeddedKafkaBroker.getTopics());
    }

    @Test
    void testConsumeDepositedEvent() throws Exception {
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
        logger.info("Serializing DepositedEvent: transactionId={}, walletId={}", transactionId, walletId);
        String json = objectMapper.writeValueAsString(event);
        logger.info("Serialized JSON: {}", json);

        // Act
        logger.info("Sending message to topic wallet-events with key={}", walletId.toString());
        kafkaTemplate.send("wallet-events", walletId.toString(), event).get(15, TimeUnit.SECONDS);
        logger.info("Message sent successfully");

        // Assert
        logger.info("Waiting for message processing");
        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            verify(walletEventConsumer, times(1)).consumeEvent(
                    argThat(record -> record instanceof ConsumerRecord &&
                            record.key().equals(walletId.toString()) &&
                            record.topic().equals("wallet-events") &&
                            record.value() instanceof DepositedEvent &&
                            ((DepositedEvent) record.value()).getTransactionId().equals(transactionId)),
                    any(Acknowledgment.class)
            );
        });
    }

    @Configuration
    static class TestConfig {
        @Bean
        public ProducerFactory<String, Object> producerFactory(EmbeddedKafkaBroker embeddedKafkaBroker, ObjectMapper objectMapper) {
            Map<String, Object> configProps = new HashMap<>();
            configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString());
            configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
            configProps.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, true);
            return new DefaultKafkaProducerFactory<>(configProps);
        }

        @Bean
        public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
            return new KafkaTemplate<>(producerFactory);
        }

        @Bean
        public ConsumerFactory<String, Object> consumerFactory(EmbeddedKafkaBroker embeddedKafkaBroker, ObjectMapper objectMapper) {
            Map<String, Object> props = new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString());
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-wallet-projection");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
            props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class.getName());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
            props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.recargapay.wallet.common.event");

            JsonDeserializer<Object> jsonDeserializer = new JsonDeserializer<>(Object.class, objectMapper, false);
            jsonDeserializer.addTrustedPackages("com.recargapay.wallet.common.event");
            logger.info("JsonDeserializer configured with trusted packages: com.recargapay.wallet.common.event");
            return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(),
                    new ErrorHandlingDeserializer<>(jsonDeserializer));
        }

        @Bean
        public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
                ConsumerFactory<String, Object> consumerFactory,
                @Qualifier("kafkaErrorHandler") CommonErrorHandler errorHandler) {
            ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(consumerFactory);
            factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
            factory.setCommonErrorHandler(errorHandler);
            factory.setConcurrency(1);
            logger.info("KafkaListenerContainerFactory configured with standard settings");
            return factory;
        }
    }
}
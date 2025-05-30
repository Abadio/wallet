package com.recargapay.wallet.query.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import jakarta.annotation.PostConstruct;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Kafka configuration for integration tests.
 */
@Testcontainers
@Configuration
@Profile("integration")
public class TestKafkaConfig {
    private static final Logger logger = LoggerFactory.getLogger(TestKafkaConfig.class);

    private static final KafkaContainer kafkaContainer = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.2.1")
    ).withStartupTimeout(Duration.ofSeconds(180));

    @PostConstruct
    public void init() {
        if (!kafkaContainer.isRunning()) {
            logger.info("Starting Kafka container...");
            kafkaContainer.start();
            logger.info("Kafka container started with bootstrap servers: {}", kafkaContainer.getBootstrapServers());
        }

        // Create wallet-events topic
        try (AdminClient adminClient = AdminClient.create(
                Map.of("bootstrap.servers", kafkaContainer.getBootstrapServers()))) {
            adminClient.createTopics(Collections.singletonList(new NewTopic("wallet-events", 1, (short) 1)))
                    .all().get(30, TimeUnit.SECONDS);
            logger.info("Topic wallet-events created");
        } catch (Exception e) {
            logger.warn("Failed to create topic wallet-events, it may already exist: {}", e.getMessage());
        }

        // Set bootstrap.servers for Spring Boot autoconfiguration
        System.setProperty("spring.kafka.bootstrap-servers", kafkaContainer.getBootstrapServers());
    }

    @Bean
    public KafkaContainer kafkaContainer() {
        return kafkaContainer;
    }

    @Bean
    public ProducerFactory<String, Object> producerFactory(ObjectMapper objectMapper) {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        configProps.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, true);
        logger.info("ProducerFactory configured with bootstrap servers: {}", kafkaContainer.getBootstrapServers());
        return new DefaultKafkaProducerFactory<>(configProps, new org.apache.kafka.common.serialization.StringSerializer(),
                new JsonSerializer<>(objectMapper));
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
        logger.info("KafkaTemplate created for tests");
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public ConsumerFactory<String, Object> consumerFactory(ObjectMapper objectMapper) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "simple-kafka-consumer-test");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.recargapay.wallet.common.event");
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, true);
        props.put(JsonDeserializer.TYPE_MAPPINGS,
                "DepositedEvent:com.recargapay.wallet.common.event.DepositedEvent," +
                        "WithdrawnEvent:com.recargapay.wallet.common.event.WithdrawnEvent," +
                        "TransferredEvent:com.recargapay.wallet.common.event.TransferredEvent");
        logger.info("ConsumerFactory configured with bootstrap servers: {}, groupId: {}",
                kafkaContainer.getBootstrapServers(), "simple-kafka-consumer-test");
        JsonDeserializer<Object> jsonDeserializer = new JsonDeserializer<>(Object.class, objectMapper);
        return new DefaultKafkaConsumerFactory<>(props, new org.apache.kafka.common.serialization.StringDeserializer(),
                new ErrorHandlingDeserializer<>(jsonDeserializer));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(org.springframework.kafka.listener.ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setConcurrency(1);
        factory.setCommonErrorHandler(kafkaErrorHandler());
        logger.info("KafkaListenerContainerFactory configured with manual acknowledgment");
        return factory;
    }

    @Bean
    public CommonErrorHandler kafkaErrorHandler() {
        return new DefaultErrorHandler((record, exception) -> {
            logger.error("Kafka consumer error: topic={}, key={}, value={}, exception={}",
                    record.topic(), record.key(), record.value(), exception.getMessage(), exception);
        });
    }
}
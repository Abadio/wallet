package com.recargapay.wallet.consumer.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

import java.util.HashMap;
import java.util.Map;

/**
 * Configures Kafka producer and consumer components for integration tests.
 * Uses EmbeddedKafkaBroker for test isolation.
 */
@Configuration
@Profile("test")
public class TestKafkaConfig {
    private static final Logger logger = LoggerFactory.getLogger(TestKafkaConfig.class);

    /**
     * Configures Kafka producer components for tests.
     *
     * @param embeddedKafkaBroker Embedded Kafka broker for test isolation
     * @param objectMapper        Jackson ObjectMapper for JSON serialization
     * @return Configured ProducerFactory
     */
    @Bean
    public ProducerFactory<String, Object> producerFactory(EmbeddedKafkaBroker embeddedKafkaBroker, ObjectMapper objectMapper) {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString());
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        configProps.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, true);
        logger.info("ProducerFactory configured with bootstrap servers: {}", embeddedKafkaBroker.getBrokersAsString());
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    /**
     * Creates a KafkaTemplate for sending messages in tests.
     *
     * @param producerFactory ProducerFactory for KafkaTemplate
     * @return Configured KafkaTemplate
     */
    @Bean
    @Primary
    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
        logger.info("KafkaTemplate created for tests");
        return new KafkaTemplate<>(producerFactory);
    }

    /**
     * Configures Kafka consumer components for tests.
     *
     * @param embeddedKafkaBroker Embedded Kafka broker for test isolation
     * @param objectMapper        Jackson ObjectMapper for JSON deserialization
     * @return Configured ConsumerFactory
     */
    @Bean
    @Primary
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
        props.put(JsonDeserializer.TYPE_MAPPINGS,
                "DepositedEvent:com.recargapay.wallet.common.event.DepositedEvent," +
                        "WithdrawnEvent:com.recargapay.wallet.common.event.WithdrawnEvent," +
                        "TransferredEvent:com.recargapay.wallet.common.event.TransferredEvent");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, "java.lang.Object");

        logger.info("ConsumerFactory configured with bootstrap servers: {}, groupId: {}", embeddedKafkaBroker.getBrokersAsString(), "test-wallet-projection");

        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(),
                new ErrorHandlingDeserializer<>(new JsonDeserializer<Object>(objectMapper)));
    }

    /**
     * Creates a Kafka listener container factory for tests with error handling and manual acknowledgment.
     *
     * @param consumerFactory ConsumerFactory for the listener
     * @param errorHandler    Error handler for Kafka listener
     * @return Configured ConcurrentKafkaListenerContainerFactory
     */
    @Bean
    @Primary
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory,
            CommonErrorHandler errorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setCommonErrorHandler(errorHandler);
        factory.setConcurrency(1);
        logger.info("KafkaListenerContainerFactory configured with manual acknowledgment and concurrency=1");
        return factory;
    }
}
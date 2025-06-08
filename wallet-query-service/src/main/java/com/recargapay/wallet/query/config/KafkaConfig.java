package com.recargapay.wallet.query.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
@Profile("!integration")
public class KafkaConfig {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConfig.class);

    @Configuration
    @Profile("!integration")
    public static class ProducerConfiguration {
        @Value("${spring.kafka.bootstrap-servers}")
        private String bootstrapServers;

        @Bean
        public ProducerFactory<String, Object> producerFactory(ObjectMapper objectMapper) {
            Map<String, Object> configProps = new HashMap<>();
            configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
            // Garante que o produtor envie as informações de tipo
            configProps.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, true);
            logger.info("ProducerFactory configured with bootstrap servers: {}", bootstrapServers);
            return new DefaultKafkaProducerFactory<>(configProps);
        }

        @Bean
        public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
            logger.info("KafkaTemplate created");
            return new KafkaTemplate<>(producerFactory);
        }
    }

    @Configuration
    @Profile("!integration")
    public static class ConsumerConfiguration {
        @Value("${spring.kafka.bootstrap-servers}")
        private String bootstrapServers;

        private Map<String, Object> baseConsumerProperties() {
            Map<String, Object> props = new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
            props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
            props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.recargapay.wallet.common.event");
            // **CORREÇÃO**: Garantindo que o deserializer use os cabeçalhos de tipo.
            props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, true);
            props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, Object.class);
            return props;
        }

        private ConcurrentKafkaListenerContainerFactory<String, Object> createListenerFactory(
                ConsumerFactory<String, Object> consumerFactory, CommonErrorHandler errorHandler) {
            ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(consumerFactory);
            factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
            factory.setCommonErrorHandler(errorHandler);
            factory.setConcurrency(1);
            return factory;
        }

        // --- Beans para cada consumidor ---

        @Bean
        public ConsumerFactory<String, Object> consumerFactory(
                @Value("${spring.kafka.consumer.main.group-id}") String groupId) {
            Map<String, Object> props = baseConsumerProperties();
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            return new DefaultKafkaConsumerFactory<>(props);
        }

        @Bean
        public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
                ConsumerFactory<String, Object> consumerFactory, CommonErrorHandler errorHandler) {
            return createListenerFactory(consumerFactory, errorHandler);
        }

        @Bean
        public ConsumerFactory<String, Object> dltConsumerFactory(
                @Value("${spring.kafka.consumer.dlt.group-id}") String groupId) {
            Map<String, Object> props = baseConsumerProperties();
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            return new DefaultKafkaConsumerFactory<>(props);
        }

        @Bean
        public ConcurrentKafkaListenerContainerFactory<String, Object> dltKafkaListenerContainerFactory(
                ConsumerFactory<String, Object> dltConsumerFactory, CommonErrorHandler errorHandler) {
            return createListenerFactory(dltConsumerFactory, errorHandler);
        }

        @Bean
        public ConsumerFactory<String, Object> failedDltConsumerFactory(
                @Value("${spring.kafka.consumer.failed-dlt.group-id}") String groupId) {
            Map<String, Object> props = baseConsumerProperties();
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            return new DefaultKafkaConsumerFactory<>(props);
        }

        @Bean
        public ConcurrentKafkaListenerContainerFactory<String, Object> failedDltKafkaListenerContainerFactory(
                ConsumerFactory<String, Object> failedDltConsumerFactory, CommonErrorHandler errorHandler) {
            return createListenerFactory(failedDltConsumerFactory, errorHandler);
        }
    }

    @Configuration
    @Profile("!integration")
    public static class ErrorHandlerConfiguration {
        @Bean
        public CommonErrorHandler kafkaErrorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
            DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                    (record, ex) -> {
                        logger.error("Error processing record, sending to DLT 'wallet-events-dlq'. Key: {}. Exception: {}", record.key(), ex.getMessage());
                        return new org.apache.kafka.common.TopicPartition("wallet-events-dlq", -1);
                    });
            return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2L));
        }
    }
}
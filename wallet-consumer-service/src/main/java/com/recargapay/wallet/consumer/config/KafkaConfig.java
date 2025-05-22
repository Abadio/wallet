package com.recargapay.wallet.consumer.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Configures Kafka producer and consumer components for the wallet consumer service.
 */
@Configuration
@Profile("!test")
public class KafkaConfig {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConfig.class);

    @Configuration
    @Profile("!test")
    public static class ProducerConfiguration {
        @Value("${spring.kafka.bootstrap-servers}")
        private String bootstrapServers;

        @Bean
        public ProducerFactory<String, Object> producerFactory(ObjectMapper objectMapper) {
            Map<String, Object> configProps = new HashMap<>();
            configProps.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            configProps.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            configProps.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
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
    @Profile("!test")
    public static class ConsumerConfiguration {
        @Value("${spring.kafka.bootstrap-servers}")
        private String bootstrapServers;

        @Value("${spring.kafka.consumer.group-id}")
        private String groupId;

        @Value("${spring.kafka.consumer.auto-offset-reset}")
        private String autoOffsetReset;

        @Value("${spring.kafka.consumer.enable-auto-commit}")
        private boolean enableAutoCommit;

        @Bean
        public ConsumerFactory<String, Object> consumerFactory(ObjectMapper objectMapper) {
            Map<String, Object> props = new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
            props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class.getName());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
            props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.recargapay.wallet.common.event");
            props.put(JsonDeserializer.TYPE_MAPPINGS,
                    "DepositedEvent:com.recargapay.wallet.common.event.DepositedEvent," +
                            "WithdrawnEvent:com.recargapay.wallet.common.event.WithdrawnEvent," +
                            "TransferredEvent:com.recargapay.wallet.common.event.TransferredEvent");

            logger.info("ConsumerFactory configured with bootstrap servers: {}, groupId: {}", bootstrapServers, groupId);

            return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(),
                    new ErrorHandlingDeserializer<>(new JsonDeserializer<>(Object.class, objectMapper)));
        }

        @Bean
        public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
                ConsumerFactory<String, Object> consumerFactory,
                CommonErrorHandler errorHandler) {
            ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(consumerFactory);
            factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
            factory.setCommonErrorHandler(errorHandler);
            factory.setConcurrency(1);
            //factory.getContainerProperties().setTransactionManager(null); // Explicitly disable transactions
            logger.info("KafkaListenerContainerFactory configured with manual acknowledgment and concurrency=1");
            return factory;
        }

        @Bean(name = "kafkaErrorHandler")
        public CommonErrorHandler kafkaErrorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
            return new KafkaErrorHandler(kafkaTemplate);
        }
    }
}
package com.recargapay.wallet.query.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Configures Kafka producer, consumer, and error handling components for the wallet query service.
 */
@Configuration
@Profile("!integration")
public class KafkaConfig {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConfig.class);

    @Configuration
    @Profile("!integration")
    public static class ProducerConfiguration {
        @Value("${spring.kafka.bootstrap-servers}")
        private String bootstrapServers;

        /**
         * Configures the Kafka ProducerFactory.
         *
         * @param objectMapper Jackson ObjectMapper for JSON serialization
         * @return Configured ProducerFactory
         */
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

        /**
         * Configures the KafkaTemplate for producing messages.
         *
         * @param producerFactory ProducerFactory for Kafka
         * @return Configured KafkaTemplate
         */
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

        @Value("${spring.kafka.consumer.group-id}")
        private String groupId;

        @Value("${spring.kafka.consumer.auto-offset-reset}")
        private String autoOffsetReset;

        @Value("${spring.kafka.consumer.enable-auto-commit}")
        private boolean enableAutoCommit;

        /**
         * Configures the Kafka ConsumerFactory.
         *
         * @param objectMapper Jackson ObjectMapper for JSON deserialization
         * @return Configured ConsumerFactory
         */
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

        /**
         * Configures the Kafka Listener Container Factory.
         *
         * @param consumerFactory ConsumerFactory for Kafka
         * @param errorHandler Error handler for Kafka listeners
         * @return Configured ConcurrentKafkaListenerContainerFactory
         */
        @Bean
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

    @Configuration
    @Profile("!integration")
    public static class ErrorHandlerConfiguration {
        /**
         * Configures the Kafka error handler.
         *
         * @param kafkaTemplate KafkaTemplate for DLQ publishing
         * @return Configured CommonErrorHandler
         */
        @Bean(name = "kafkaErrorHandler")
        public CommonErrorHandler kafkaErrorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
            return new KafkaErrorHandler(kafkaTemplate);
        }
    }

    /**
     * Handles Kafka consumer errors, including deserialization and processing errors.
     * Retries failed messages up to a maximum number of attempts and sends unprocessable messages to a DLQ.
     */
    public static class KafkaErrorHandler extends DefaultErrorHandler {
        private static final Logger LOGGER = LoggerFactory.getLogger(KafkaErrorHandler.class);

        /**
         * Configures the error handler with retry policies and DLQ publishing.
         *
         * @param kafkaTemplate Kafka template for sending messages to the DLQ
         */
        public KafkaErrorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
            super(new DeadLetterPublishingRecoverer(kafkaTemplate, (record, ex) -> {
                LOGGER.error("Sending to DLQ: topic={}, partition={}, offset={}, key={}, value={}, error={}",
                        record.topic(), record.partition(), record.offset(), record.key(), record.value(), ex.getMessage());
                return new org.apache.kafka.common.TopicPartition("wallet-events-dlq", record.partition());
            }), new FixedBackOff(1000L, 3));
            LOGGER.info("KafkaErrorHandler initialized with 3 retries and DLQ support");
        }

        /**
         * Handles remaining errors by logging them before delegating to the default handler.
         */
        @Override
        public void handleRemaining(Exception thrownException, List<ConsumerRecord<?, ?>> records,
                                    Consumer<?, ?> consumer, MessageListenerContainer container) {
            if (!records.isEmpty()) {
                ConsumerRecord<?, ?> record = records.get(0);
                LOGGER.error("Error processing Kafka record: topic={}, partition={}, offset={}, key={}, value={}, error={}",
                        record.topic(), record.partition(), record.offset(), record.key(), record.value(), thrownException.getMessage());
            }
            super.handleRemaining(thrownException, records, consumer, container);
        }
    }
}
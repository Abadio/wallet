package com.recargapay.wallet.query.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
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
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import jakarta.annotation.PostConstruct;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Testcontainers
@Configuration
@Profile("integration")
public class TestKafkaConfig {
    private static final Logger logger = LoggerFactory.getLogger(TestKafkaConfig.class);

    private static final KafkaContainer kafkaContainer = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.2.1")
    ).withStartupTimeout(Duration.ofSeconds(300))
            .withExposedPorts(9092, 9093)
            .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
            .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
            .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
            .withEnv("KAFKA_LISTENERS", "PLAINTEXT://0.0.0.0:9093,BROKER://0.0.0.0:9092")
            .withEnv("KAFKA_ADVERTISED_LISTENERS", "PLAINTEXT://localhost:9093,BROKER://localhost:9092")
            .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER")
            .withEnv("KAFKA_HEAP_OPTS", "-Xmx256m -Xms256m") // Reduced memory
            .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withMemory(512L * 1024 * 1024)) // 512MB
            .withReuse(true);

    static {
        try {
            if (!kafkaContainer.isRunning()) {
                logger.info("Starting KafkaContainer...");
                kafkaContainer.start();
                logger.info("KafkaContainer started with bootstrap servers: {}", kafkaContainer.getBootstrapServers());
                System.setProperty("spring.kafka.bootstrap-servers", kafkaContainer.getBootstrapServers());
            } else {
                logger.info("KafkaContainer already running with bootstrap servers: {}", kafkaContainer.getBootstrapServers());
            }
        } catch (Exception e) {
            logger.error("Failed to start KafkaContainer: {}", e.getMessage(), e);
            throw new RuntimeException("KafkaContainer initialization failed", e);
        }
    }

    @PostConstruct
    public void initTopics() {
        logger.info("Creating Kafka topics...");
        try (AdminClient adminClient = AdminClient.create(
                Map.of("bootstrap.servers", kafkaContainer.getBootstrapServers()))) {

            var topics = Arrays.asList(
                    new NewTopic("wallet-events", 1, (short) 1),
                    new NewTopic("wallet-events-dlq", 1, (short) 1),
                    new NewTopic("wallet-events-dlq-failed", 1, (short) 1)
            );

            adminClient.createTopics(topics).all().get(30, TimeUnit.SECONDS);
            logger.info("Topics created: wallet-events, wallet-events-dlq, wallet-events-dlq-failed");
        } catch (Exception e) {
            logger.warn("Failed to create topics, they may already exist: {}", e.getMessage());
        }
    }

    @PostConstruct
    public void verifyKafkaContainer() {
        if (!kafkaContainer.isRunning()) {
            logger.error("Kafka container is not running!");
            throw new IllegalStateException("Kafka container failed to start");
        }
        logger.info("Kafka container is running with bootstrap servers: {}", kafkaContainer.getBootstrapServers());
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
        configProps.put("spring.json.type.mapping",
                "DepositedEvent:com.recargapay.wallet.common.event.DepositedEvent," +
                        "WithdrawnEvent:com.recargapay.wallet.common.event.WithdrawnEvent," +
                        "TransferredEvent:com.recargapay.wallet.common.event.TransferredEvent");
        logger.info("ProducerFactory configured with bootstrap servers: {}", kafkaContainer.getBootstrapServers());
        return new DefaultKafkaProducerFactory<>(configProps, new org.apache.kafka.common.serialization.StringSerializer(),
                new JsonSerializer<>(objectMapper));
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
        logger.info("KafkaTemplate created for tests");
        return new KafkaTemplate<>(producerFactory);
    }

    private Map<String, Object> createConsumerProperties(String groupId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1); // Process one message at a time
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 100); // Short wait to reduce memory
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 30000); // Reduced to avoid long sessions
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000); // Stable session timeout
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000); // Stable heartbeat
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.recargapay.wallet.common.event");
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, true);
        props.put(JsonDeserializer.REMOVE_TYPE_INFO_HEADERS, false);
        props.put(JsonDeserializer.TYPE_MAPPINGS,
                "DepositedEvent:com.recargapay.wallet.common.event.DepositedEvent," +
                        "WithdrawnEvent:com.recargapay.wallet.common.event.WithdrawnEvent," +
                        "TransferredEvent:com.recargapay.wallet.common.event.TransferredEvent");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, "java.lang.Object");
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 128 * 1024); // 128KB
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 64 * 1024); // 64KB
        logger.debug("Consumer properties created for groupId: {}", groupId);
        return props;
    }

    @Bean
    public ConsumerFactory<String, Object> consumerFactory(
            ObjectMapper objectMapper,
            @Value("${kafka.consumer.main.group-id}") String groupId) {
        Map<String, Object> props = createConsumerProperties(groupId);
        logger.info("ConsumerFactory configured with bootstrap servers: {}, groupId: {}",
                kafkaContainer.getBootstrapServers(), groupId);
        return new DefaultKafkaConsumerFactory<>(props, new org.apache.kafka.common.serialization.StringDeserializer(),
                new ErrorHandlingDeserializer<>(new JsonDeserializer<>(objectMapper)));
    }

    @Bean
    public ConsumerFactory<String, Object> dltConsumerFactory(
            ObjectMapper objectMapper,
            @Value("${kafka.consumer.dlt.group-id}") String groupId) {
        Map<String, Object> props = createConsumerProperties(groupId);
        logger.info("DLT ConsumerFactory configured with bootstrap servers: {}, groupId: {}",
                kafkaContainer.getBootstrapServers(), groupId);
        return new DefaultKafkaConsumerFactory<>(props, new org.apache.kafka.common.serialization.StringDeserializer(),
                new ErrorHandlingDeserializer<>(new JsonDeserializer<>(objectMapper)));
    }

    @Bean
    public ConsumerFactory<String, Object> failedDltConsumerFactory(
            ObjectMapper objectMapper,
            @Value("${kafka.consumer.failed-dlt.group-id}") String groupId) {
        Map<String, Object> props = createConsumerProperties(groupId);
        logger.info("Failed DLT ConsumerFactory configured with bootstrap servers: {}, groupId: {}",
                kafkaContainer.getBootstrapServers(), groupId);
        return new DefaultKafkaConsumerFactory<>(props, new org.apache.kafka.common.serialization.StringDeserializer(),
                new ErrorHandlingDeserializer<>(new JsonDeserializer<>(objectMapper)));
    }

    private ConcurrentKafkaListenerContainerFactory<String, Object> createListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory, CommonErrorHandler errorHandler, String factoryName) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setConcurrency(1); // Single consumer thread
        factory.getContainerProperties().setPollTimeout(1000); // Aumentado para 1000ms
        factory.setCommonErrorHandler(errorHandler);
        factory.getContainerProperties().setConsumerStartTimeout(Duration.ofSeconds(30));
        logger.info("{} KafkaListenerContainerFactory configured with concurrency=1, pollTimeout=1000ms", factoryName);
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory,
            CommonErrorHandler errorHandler) {
        return createListenerContainerFactory(consumerFactory, errorHandler, "Main");
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> dltKafkaListenerContainerFactory(
            ConsumerFactory<String, Object> dltConsumerFactory,
            CommonErrorHandler errorHandler) {
        return createListenerContainerFactory(dltConsumerFactory, errorHandler, "DLT");
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> failedDltKafkaListenerContainerFactory(
            ConsumerFactory<String, Object> failedDltConsumerFactory,
            CommonErrorHandler errorHandler) {
        return createListenerContainerFactory(failedDltConsumerFactory, errorHandler, "Failed DLT");
    }

    @Bean
    public CommonErrorHandler kafkaErrorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, exception) -> {
                    logger.warn("Sending record to DLT: topic=wallet-events-dlq, key={}, value={}, exception={}",
                            record.key(), record.value() != null ? record.value().getClass().getSimpleName() : "null", exception.getMessage());
                    // CORREÇÃO: Enviando para o tópico DLT correto
                    return new TopicPartition("wallet-events-dlq", -1);
                }
        );

        // ... (resto do errorHandler permanece o mesmo) ...
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                recoverer,
                new FixedBackOff(5000L, 0L) // Disable automatic retries
        );
        errorHandler.setCommitRecovered(true);
        logger.info("DefaultErrorHandler configured with DLT support and no automatic retries");
        return errorHandler;
    }
}
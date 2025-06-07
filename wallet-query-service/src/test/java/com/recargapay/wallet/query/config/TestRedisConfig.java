package com.recargapay.wallet.query.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.testcontainers.RedisContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import jakarta.annotation.PostConstruct;

@Testcontainers
@Configuration
@Profile("integration")
public class TestRedisConfig {
    private static final Logger logger = LoggerFactory.getLogger(TestRedisConfig.class);

    @Container
    private static final RedisContainer redisContainer = new RedisContainer(RedisContainer.DEFAULT_IMAGE_NAME.withTag("7.0.12"))
            .withExposedPorts(6379)
            .withCommand("redis-server --maxmemory 256mb") // Limit Redis memory
            .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withMemory(512L * 1024 * 1024)) // 512MB
            .withReuse(true);

    @PostConstruct
    public void init() {
        try {
            if (!redisContainer.isRunning()) {
                logger.info("Starting Redis container...");
                redisContainer.start();
                logger.info("Redis container started on host: {}, port: {}", redisContainer.getHost(), redisContainer.getMappedPort(6379));
            } else {
                logger.info("Redis container already running on host: {}, port: {}", redisContainer.getHost(), redisContainer.getMappedPort(6379));
            }
        } catch (Exception e) {
            logger.error("Failed to start Redis container: {}", e.getMessage(), e);
            throw new IllegalStateException("Failed to start Redis container", e);
        }
    }

    @Bean
    public RedisContainer redisContainer() {
        return redisContainer;
    }

    @Bean
    public LettuceConnectionFactory redisConnectionFactory() {
        RedisStandaloneConfiguration config = new RedisStandaloneConfiguration();
        config.setHostName(redisContainer.getHost());
        config.setPort(redisContainer.getMappedPort(6379));
        config.setDatabase(0);
        logger.info("Configuring LettuceConnectionFactory with host: {}, port: {}", redisContainer.getHost(), redisContainer.getMappedPort(6379));
        return new LettuceConnectionFactory(config);
    }

    @Bean
    public RedisTemplate<String, Object> redisTemplate(LettuceConnectionFactory connectionFactory, ObjectMapper objectMapper) {
        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);

        template.setKeySerializer(new StringRedisSerializer());

        // CORREÇÃO: Usando o construtor correto em vez do método depreciado setObjectMapper
        Jackson2JsonRedisSerializer<Object> serializer = new Jackson2JsonRedisSerializer<>(objectMapper, Object.class);

        template.setValueSerializer(serializer);
        template.setHashValueSerializer(serializer);

        template.afterPropertiesSet();
        return template;
    }
}
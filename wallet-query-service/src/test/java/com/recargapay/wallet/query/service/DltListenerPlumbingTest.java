package com.recargapay.wallet.query.service;

import com.recargapay.wallet.common.event.DepositedEvent;
import com.recargapay.wallet.common.event.TransferredEvent;
import com.recargapay.wallet.common.event.WithdrawnEvent;
import com.recargapay.wallet.query.dlt.RedisDltConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@SpringBootTest
@ActiveProfiles("integration")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class DltListenerPlumbingTest {

    private static final Logger logger = LoggerFactory.getLogger(DltListenerPlumbingTest.class);

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    @SpyBean
    private CacheService cacheService;

    @SpyBean
    private RedisDltConsumer redisDltConsumer;

    @BeforeEach
    void setUp() {
        logger.info("--- Test Setup: Clearing state ---");
        Mockito.clearInvocations(cacheService, redisDltConsumer);
        redisTemplate.getConnectionFactory().getConnection().flushDb();
    }

    private ProducerRecord<String, Object> createProducerRecord(String key, Object event) {
        ProducerRecord<String, Object> record = new ProducerRecord<>(RedisDltConsumer.getDltTopic(), key, event);
        record.headers().add("message-id", UUID.randomUUID().toString().getBytes());
        record.headers().add("error-message", "timeout".getBytes()); // Para passar na checagem de erro transiente
        return record;
    }

    @Test
    @DisplayName("Deve consumir DepositedEvent do DLT e invalidar o cache (E2E)")
    void shouldConsumeDepositedEventAndInvalidateCache() throws Exception {
        logger.info("--- Starting test: shouldConsumeDepositedEventAndInvalidateCache ---");
        // 1. Setup
        UUID walletId = UUID.randomUUID();
        DepositedEvent event = new DepositedEvent(UUID.randomUUID(), walletId, BigDecimal.TEN, BigDecimal.TEN, "", OffsetDateTime.now());
        ProducerRecord<String, Object> record = createProducerRecord(walletId.toString(), event);

        redisTemplate.opsForValue().set("wallet:balance:" + walletId.toString(), "dummy-value");
        doNothing().when(cacheService).invalidateCache(walletId);

        // 2. Execução
        logger.info("Sending DepositedEvent to DLT topic for walletId: {}", walletId);
        kafkaTemplate.send(record).get(10, TimeUnit.SECONDS);

        // 3. Verificação
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            verify(redisDltConsumer, atLeastOnce()).consumeDlt(any(), any());
            verify(cacheService, times(1)).invalidateCache(walletId);
            logger.info("Verification successful!");
        });

        logger.info("--- Test Finished ---");
    }

    @Test
    @DisplayName("Deve consumir WithdrawnEvent do DLT e invalidar o cache (E2E)")
    void shouldConsumeWithdrawnEventAndInvalidateCache() throws Exception {
        logger.info("--- Starting test: shouldConsumeWithdrawnEventAndInvalidateCache ---");
        // 1. Setup
        UUID walletId = UUID.randomUUID();
        WithdrawnEvent event = new WithdrawnEvent(UUID.randomUUID(), walletId, BigDecimal.TEN, BigDecimal.TEN, "", OffsetDateTime.now());
        ProducerRecord<String, Object> record = createProducerRecord(walletId.toString(), event);

        redisTemplate.opsForValue().set("wallet:balance:" + walletId.toString(), "dummy-value");
        doNothing().when(cacheService).invalidateCache(walletId);

        // 2. Execução
        logger.info("Sending WithdrawnEvent to DLT topic for walletId: {}", walletId);
        kafkaTemplate.send(record).get(10, TimeUnit.SECONDS);

        // 3. Verificação
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            verify(redisDltConsumer, atLeastOnce()).consumeDlt(any(), any());
            verify(cacheService, times(1)).invalidateCache(walletId);
            logger.info("Verification successful!");
        });

        logger.info("--- Test Finished ---");
    }

    @Test
    @DisplayName("Deve consumir TransferredEvent do DLT e invalidar ambos os caches (E2E)")
    void shouldConsumeTransferredEventAndInvalidateBothCaches() throws Exception {
        logger.info("--- Starting test: shouldConsumeTransferredEventAndInvalidateBothCaches ---");
        // 1. Setup
        UUID fromWalletId = UUID.randomUUID();
        UUID toWalletId = UUID.randomUUID();
        TransferredEvent event = new TransferredEvent(UUID.randomUUID(), fromWalletId, toWalletId, BigDecimal.TEN, BigDecimal.ONE, BigDecimal.ONE, "", OffsetDateTime.now(), "USD");
        // A chave do registro do Kafka é a carteira de origem
        ProducerRecord<String, Object> record = createProducerRecord(fromWalletId.toString(), event);

        // A verificação de existência da chave no Redis usa a chave do registro
        redisTemplate.opsForValue().set("wallet:balance:" + fromWalletId.toString(), "dummy-value");

        // Mockamos o comportamento para ambas as carteiras
        doNothing().when(cacheService).invalidateCache(fromWalletId);
        doNothing().when(cacheService).invalidateCache(toWalletId);

        // 2. Execução
        logger.info("Sending TransferredEvent to DLT topic for fromWalletId: {}", fromWalletId);
        kafkaTemplate.send(record).get(10, TimeUnit.SECONDS);

        // 3. Verificação
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            verify(redisDltConsumer, atLeastOnce()).consumeDlt(any(), any());
            // Verifica que AMBOS os caches foram invalidados
            verify(cacheService, times(1)).invalidateCache(fromWalletId);
            verify(cacheService, times(1)).invalidateCache(toWalletId);
            logger.info("Verification successful!");
        });

        logger.info("--- Test Finished ---");
    }
}
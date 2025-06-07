package com.recargapay.wallet.query.service;

import com.recargapay.wallet.common.event.DepositedEvent;
import com.recargapay.wallet.common.event.TransferredEvent;
import com.recargapay.wallet.common.event.WithdrawnEvent;
import com.recargapay.wallet.query.dlt.RedisDltConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
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
import static org.junit.jupiter.api.Assertions.assertNull;

@SpringBootTest
@ActiveProfiles("integration")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class CacheInvalidationPlumbingTest {

    private static final Logger logger = LoggerFactory.getLogger(CacheInvalidationPlumbingTest.class);

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    // **REMOVIDO**: Não usamos mais spy no consumidor para este teste.
    // @SpyBean
    // private CacheInvalidationConsumer cacheInvalidationConsumer;

    @BeforeEach
    void setUp() {
        logger.info("--- Test Setup: Clearing Redis state ---");
        redisTemplate.getConnectionFactory().getConnection().flushDb();
    }

    private ProducerRecord<String, Object> createProducerRecord(String key, Object event) {
        return new ProducerRecord<>("wallet-events", key, event);
    }

    @Test
    @DisplayName("Deve consumir DepositedEvent e invalidar cache no Redis (E2E)")
    void shouldConsumeDepositedEventAndInvalidateCache() throws Exception {
        logger.info("--- Starting test: shouldConsumeDepositedEventAndInvalidateCache ---");
        // 1. Setup
        UUID walletId = UUID.randomUUID();
        String balanceKey = "wallet:balance:" + walletId;
        DepositedEvent event = new DepositedEvent(UUID.randomUUID(), walletId, BigDecimal.TEN, BigDecimal.TEN, "", OffsetDateTime.now());
        ProducerRecord<String, Object> record = createProducerRecord(walletId.toString(), event);

        redisTemplate.opsForValue().set(balanceKey, "dummy-balance-value");

        // 2. Execução
        logger.info("Sending DepositedEvent to 'wallet-events' topic for walletId: {}", walletId);
        kafkaTemplate.send(record).get(10, TimeUnit.SECONDS);

        // 3. Verificação
        // A verificação agora é no RESULTADO FINAL (o estado do Redis), não no mock.
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertNull(redisTemplate.opsForValue().get(balanceKey), "O cache de saldo deveria ter sido invalidado");
            logger.info("Verification successful! Cache key {} was deleted.", balanceKey);
        });
        logger.info("--- Test Finished ---");
    }

    @Test
    @DisplayName("Deve consumir WithdrawnEvent e invalidar cache no Redis (E2E)")
    void shouldConsumeWithdrawnEventAndInvalidateCache() throws Exception {
        logger.info("--- Starting test: shouldConsumeWithdrawnEventAndInvalidateCache ---");
        // 1. Setup
        UUID walletId = UUID.randomUUID();
        String balanceKey = "wallet:balance:" + walletId;
        WithdrawnEvent event = new WithdrawnEvent(UUID.randomUUID(), walletId, BigDecimal.TEN, BigDecimal.TEN, "", OffsetDateTime.now());
        ProducerRecord<String, Object> record = createProducerRecord(walletId.toString(), event);

        redisTemplate.opsForValue().set(balanceKey, "dummy-balance-value");

        // 2. Execução
        logger.info("Sending WithdrawnEvent to 'wallet-events' topic for walletId: {}", walletId);
        kafkaTemplate.send(record).get(10, TimeUnit.SECONDS);

        // 3. Verificação
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertNull(redisTemplate.opsForValue().get(balanceKey), "O cache de saldo deveria ter sido invalidado");
            logger.info("Verification successful! Cache key {} was deleted.", balanceKey);
        });
        logger.info("--- Test Finished ---");
    }

    @Test
    @DisplayName("Deve consumir TransferredEvent e invalidar ambos os caches no Redis (E2E)")
    void shouldConsumeTransferredEventAndInvalidateBothCaches() throws Exception {
        logger.info("--- Starting test: shouldConsumeTransferredEventAndInvalidateBothCaches ---");
        // 1. Setup
        UUID fromWalletId = UUID.randomUUID();
        UUID toWalletId = UUID.randomUUID();
        String fromBalanceKey = "wallet:balance:" + fromWalletId;
        String toBalanceKey = "wallet:balance:" + toWalletId;

        TransferredEvent event = new TransferredEvent(UUID.randomUUID(), fromWalletId, toWalletId, BigDecimal.TEN, BigDecimal.ONE, BigDecimal.ONE, "", OffsetDateTime.now(), "USD");
        ProducerRecord<String, Object> record = createProducerRecord(fromWalletId.toString(), event);

        redisTemplate.opsForValue().set(fromBalanceKey, "dummy-from-wallet");
        redisTemplate.opsForValue().set(toBalanceKey, "dummy-to-wallet");

        // 2. Execução
        logger.info("Sending TransferredEvent to 'wallet-events' topic for fromWalletId: {}", fromWalletId);
        kafkaTemplate.send(record).get(10, TimeUnit.SECONDS);

        // 3. Verificação
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertNull(redisTemplate.opsForValue().get(fromBalanceKey), "O cache da carteira de origem deveria ter sido invalidado");
            assertNull(redisTemplate.opsForValue().get(toBalanceKey), "O cache da carteira de destino deveria ter sido invalidado");
            logger.info("Verification successful! Cache keys {} and {} were deleted.", fromBalanceKey, toBalanceKey);
        });
        logger.info("--- Test Finished ---");
    }
}
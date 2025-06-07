package com.recargapay.wallet.query.service;

import com.recargapay.wallet.common.event.DepositedEvent;
import com.recargapay.wallet.common.event.TransferredEvent;
import com.recargapay.wallet.common.event.WithdrawnEvent;
import com.recargapay.wallet.query.dlt.RedisDltConsumer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RedisDltConsumerComponentTest {

    private static final Logger logger = LoggerFactory.getLogger(RedisDltConsumerComponentTest.class);

    @Mock
    private CacheService cacheService;
    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;
    @Mock
    private MeterRegistry meterRegistry;
    @Mock
    private RedisTemplate<String, Object> redisTemplate;
    @Mock
    private ValueOperations<String, Object> valueOperations;
    @Mock
    private Counter counter;

    @InjectMocks
    private RedisDltConsumer redisDltConsumer;

    @BeforeEach
    void setUp() {
        logger.info("--- Test Setup: Initializing mocks ---");
        lenient().when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        lenient().when(meterRegistry.counter(anyString(), anyString(), anyString())).thenReturn(counter);
        lenient().when(meterRegistry.counter(anyString(), anyString(), anyString(), anyString(), anyString())).thenReturn(counter);
        // Garante que a verificação de idempotência permita a execução do teste por padrão
        lenient().when(valueOperations.setIfAbsent(anyString(), any(), anyLong(), any(TimeUnit.class))).thenReturn(true);
    }

    private ConsumerRecord<String, Object> createInputRecord(String key, Object event, int retryCount, String errorMessage) {
        ConsumerRecord<String, Object> record = new ConsumerRecord<>("wallet-query-dlt", 0, 0, key, event);
        record.headers().add("message-id", UUID.randomUUID().toString().getBytes());
        record.headers().add("retry-count", String.valueOf(retryCount).getBytes());
        if (errorMessage != null) {
            record.headers().add("error-message", errorMessage.getBytes());
        }
        return record;
    }

    // ===================================================================
    // =================== TESTES DE LÓGICA DE DESCARTE ===================
    // ===================================================================

    @Test
    @DisplayName("Deve Descartar Mensagem se Chave de Saldo Não Existe")
    void shouldDiscardMessageWhenBalanceKeyIsMissing() {
        logger.info("--- Test: shouldDiscardMessageWhenBalanceKeyIsMissing ---");
        when(redisTemplate.hasKey(anyString())).thenReturn(false);
        Acknowledgment ack = mock(Acknowledgment.class);
        ConsumerRecord<String, Object> record = createInputRecord("some-key", new DepositedEvent(), 0, "timeout");

        redisDltConsumer.consumeDlt(record, ack);

        verify(cacheService, never()).invalidateCache(any());
        verify(meterRegistry).counter("dlt.redis.discarded", "reason", "keys_expired", "eventType", "DepositedEvent");
        verify(counter).increment();
        verify(ack).acknowledge();
    }

    @Test
    @DisplayName("Deve Descartar Mensagem se o Evento for Nulo")
    void shouldDiscardNullEvent() {
        logger.info("--- Test: shouldDiscardNullEvent ---");
        Acknowledgment ack = mock(Acknowledgment.class);
        ConsumerRecord<String, Object> record = createInputRecord("some-key", null, 0, "timeout");

        redisDltConsumer.consumeDlt(record, ack);

        verify(cacheService, never()).invalidateCache(any());
        verify(meterRegistry).counter("dlt.redis.discarded", "reason", "null_event", "eventType", "unknown");
        verify(counter).increment();
        verify(ack).acknowledge();
    }

    @Test
    @DisplayName("Deve Descartar Mensagem se o Evento Não for Suportado")
    void shouldDiscardUnsupportedEvent() {
        logger.info("--- Test: shouldDiscardUnsupportedEvent ---");
        when(redisTemplate.hasKey(anyString())).thenReturn(true);
        Acknowledgment ack = mock(Acknowledgment.class);
        // Usando um objeto genérico para simular um evento não suportado
        ConsumerRecord<String, Object> record = createInputRecord("some-key", new Object(), 0, "timeout");

        redisDltConsumer.consumeDlt(record, ack);

        verify(cacheService, never()).invalidateCache(any());
        verify(meterRegistry).counter("dlt.redis.discarded", "reason", "unsupported_event", "eventType", "Object");
        verify(counter).increment();
        verify(ack).acknowledge();
    }

    @Test
    @DisplayName("Deve Ignorar Mensagem se Já Foi Processada (Idempotência)")
    void shouldSkipAlreadyProcessedMessage() {
        logger.info("--- Test: shouldSkipAlreadyProcessedMessage ---");
        // Simula que a chave de idempotência já existe no Redis
        when(valueOperations.setIfAbsent(anyString(), any(), anyLong(), any(TimeUnit.class))).thenReturn(false);
        Acknowledgment ack = mock(Acknowledgment.class);
        ConsumerRecord<String, Object> record = createInputRecord("some-key", new DepositedEvent(), 0, "timeout");

        redisDltConsumer.consumeDlt(record, ack);

        // Nenhuma lógica de negócio deve ser chamada
        verifyNoInteractions(cacheService);
        verifyNoInteractions(meterRegistry);
        verify(ack).acknowledge();
    }

    // ===================================================================
    // =================== TESTES DE LÓGICA DE RETENTATIVA ===================
    // ===================================================================

    @Test
    @DisplayName("Deve Agendar Retentativa para DepositedEvent se a Invalidação Falhar")
    void shouldScheduleRetryWhenCacheInvalidationFails() {
        logger.info("--- Test: shouldScheduleRetryWhenCacheInvalidationFails (DepositedEvent) ---");
        UUID walletId = UUID.randomUUID();
        when(redisTemplate.hasKey(anyString())).thenReturn(true);
        doThrow(new RuntimeException("Connection refused")).when(cacheService).invalidateCache(walletId);
        Acknowledgment ack = mock(Acknowledgment.class);
        ConsumerRecord<String, Object> record = createInputRecord(walletId.toString(), new DepositedEvent(UUID.randomUUID(), walletId, BigDecimal.TEN, BigDecimal.TEN, "", OffsetDateTime.now()), 0, "timeout");

        redisDltConsumer.consumeDlt(record, ack);

        verify(cacheService).invalidateCache(walletId);
        verify(kafkaTemplate).send(any(ProducerRecord.class));
        verify(meterRegistry).counter("dlt.redis.retries", "eventType", "DepositedEvent");
        verify(ack).acknowledge();
    }

    @Test
    @DisplayName("Deve Agendar Retentativa para WithdrawnEvent se a Invalidação Falhar")
    void shouldScheduleRetryForWithdrawnEvent() {
        logger.info("--- Test: shouldScheduleRetryForWithdrawnEvent ---");
        UUID walletId = UUID.randomUUID();
        when(redisTemplate.hasKey(anyString())).thenReturn(true);
        doThrow(new RuntimeException("Connection refused")).when(cacheService).invalidateCache(walletId);
        Acknowledgment ack = mock(Acknowledgment.class);
        ConsumerRecord<String, Object> record = createInputRecord(walletId.toString(), new WithdrawnEvent(UUID.randomUUID(), walletId, BigDecimal.TEN, BigDecimal.TEN, "", OffsetDateTime.now()), 0, "timeout");

        redisDltConsumer.consumeDlt(record, ack);

        verify(cacheService).invalidateCache(walletId);
        verify(kafkaTemplate).send(any(ProducerRecord.class));
        verify(meterRegistry).counter("dlt.redis.retries", "eventType", "WithdrawnEvent");
        verify(ack).acknowledge();
    }

    @Test
    @DisplayName("Deve Agendar Retentativa para TransferredEvent se a Invalidação Falhar")
    void shouldHandleTransferredEventRetryCorrectly() {
        logger.info("--- Test: shouldHandleTransferredEventRetryCorrectly ---");
        UUID fromWalletId = UUID.randomUUID();
        UUID toWalletId = UUID.randomUUID();
        when(redisTemplate.hasKey(anyString())).thenReturn(true);
        doThrow(new RuntimeException("Connection refused")).when(cacheService).invalidateCache(fromWalletId);
        Acknowledgment ack = mock(Acknowledgment.class);
        TransferredEvent event = new TransferredEvent(UUID.randomUUID(), fromWalletId, toWalletId, BigDecimal.TEN, BigDecimal.ONE, BigDecimal.ONE, "", OffsetDateTime.now(), "USD");
        ConsumerRecord<String, Object> record = createInputRecord(fromWalletId.toString(), event, 0, "timeout");

        redisDltConsumer.consumeDlt(record, ack);

        verify(cacheService, times(1)).invalidateCache(fromWalletId);
        verify(cacheService, never()).invalidateCache(toWalletId);

        ArgumentCaptor<ProducerRecord<String, Object>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        assertEquals(RedisDltConsumer.getDltTopic(), captor.getValue().topic());

        verify(meterRegistry).counter("dlt.redis.retries", "eventType", "TransferredEvent");
        verify(ack).acknowledge();
    }

    // ===================================================================
    // =================== TESTES DE LÓGICA DE ERRO FINAL =================
    // ===================================================================

    @Test
    @DisplayName("Deve Redirecionar para Fila de Falhas Após Máximo de Retentativas")
    void shouldRedirectToFailedTopicAfterMaxRetries() {
        logger.info("--- Test: shouldRedirectToFailedTopicAfterMaxRetries ---");
        UUID walletId = UUID.randomUUID();
        when(redisTemplate.hasKey(anyString())).thenReturn(true);
        doThrow(new RuntimeException("Connection refused")).when(cacheService).invalidateCache(walletId);
        Acknowledgment ack = mock(Acknowledgment.class);
        ConsumerRecord<String, Object> record = createInputRecord(walletId.toString(), new DepositedEvent(UUID.randomUUID(), walletId, BigDecimal.TEN, BigDecimal.TEN, "", OffsetDateTime.now()), 2, "timeout");

        redisDltConsumer.consumeDlt(record, ack);

        verify(cacheService).invalidateCache(walletId);

        ArgumentCaptor<ProducerRecord<String, Object>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        assertEquals(RedisDltConsumer.getFailedDltTopic(), captor.getValue().topic());

        verify(meterRegistry).counter("dlt.redis.redirected", "eventType", "DepositedEvent", "reason", "permanent_error");
        verify(ack).acknowledge();
    }

    @Test
    @DisplayName("Deve Redirecionar para Fila de Falhas Imediatamente se o Erro Não For Transiente")
    void shouldRedirectNonTransientErrorToFailedTopic() {
        logger.info("--- Test: shouldRedirectNonTransientErrorToFailedTopic ---");
        UUID walletId = UUID.randomUUID();
        when(redisTemplate.hasKey(anyString())).thenReturn(true);
        Acknowledgment ack = mock(Acknowledgment.class);
        // Criamos um erro que não contém "timeout" ou "Connection refused"
        ConsumerRecord<String, Object> record = createInputRecord(walletId.toString(), new DepositedEvent(), 0, "Validation Error");

        redisDltConsumer.consumeDlt(record, ack);

        // Não deve nem tentar chamar a lógica de negócio
        verify(cacheService, never()).invalidateCache(any());

        ArgumentCaptor<ProducerRecord<String, Object>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        assertEquals(RedisDltConsumer.getFailedDltTopic(), captor.getValue().topic());

        verify(meterRegistry).counter("dlt.redis.redirected", "eventType", "DepositedEvent", "reason", "permanent_error");
        verify(ack).acknowledge();
    }
}
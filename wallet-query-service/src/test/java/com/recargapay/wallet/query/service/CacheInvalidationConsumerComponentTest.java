package com.recargapay.wallet.query.service;

import com.recargapay.wallet.common.event.DepositedEvent;
import com.recargapay.wallet.common.event.TransferredEvent;
import com.recargapay.wallet.common.event.WithdrawnEvent;
import com.recargapay.wallet.query.dlt.RedisDltConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.Acknowledgment;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CacheInvalidationConsumerComponentTest {

    private static final Logger logger = LoggerFactory.getLogger(CacheInvalidationConsumerComponentTest.class);

    @Mock
    private CacheService cacheService;

    @InjectMocks
    private CacheInvalidationConsumer cacheInvalidationConsumer;

    // Helper para criar registros de teste de forma consistente
    private ConsumerRecord<String, Object> createRecord(String key, Object event) {
        return new ConsumerRecord<>("wallet-events", 0, 0, key, event);
    }

    // ===================================================================
    // =================== TESTES DE "CAMINHO FELIZ" =====================
    // ===================================================================

    @Test
    @DisplayName("Deve invalidar cache para DepositedEvent")
    void shouldInvalidateCacheForDepositedEvent() {
        logger.info("--- Teste: Caminho feliz para DepositedEvent ---");
        // Setup
        UUID walletId = UUID.randomUUID();
        DepositedEvent event = new DepositedEvent(UUID.randomUUID(), walletId, BigDecimal.TEN, BigDecimal.TEN, "", OffsetDateTime.now());
        ConsumerRecord<String, Object> record = createRecord(walletId.toString(), event);
        Acknowledgment ack = mock(Acknowledgment.class);
        doNothing().when(cacheService).invalidateCache(walletId);

        // Execução
        cacheInvalidationConsumer.processEvent(record, ack);

        // Verificação
        verify(cacheService, times(1)).invalidateCache(walletId);
        verify(ack, times(1)).acknowledge();
    }

    @Test
    @DisplayName("Deve invalidar cache para WithdrawnEvent")
    void shouldInvalidateCacheForWithdrawnEvent() {
        logger.info("--- Teste: Caminho feliz para WithdrawnEvent ---");
        // Setup
        UUID walletId = UUID.randomUUID();
        WithdrawnEvent event = new WithdrawnEvent(UUID.randomUUID(), walletId, BigDecimal.TEN, BigDecimal.TEN, "", OffsetDateTime.now());
        ConsumerRecord<String, Object> record = createRecord(walletId.toString(), event);
        Acknowledgment ack = mock(Acknowledgment.class);
        doNothing().when(cacheService).invalidateCache(walletId);

        // Execução
        cacheInvalidationConsumer.processEvent(record, ack);

        // Verificação
        verify(cacheService, times(1)).invalidateCache(walletId);
        verify(ack, times(1)).acknowledge();
    }

    @Test
    @DisplayName("Deve invalidar ambos os caches para TransferredEvent")
    void shouldInvalidateBothCachesForTransferredEvent() {
        logger.info("--- Teste: Caminho feliz para TransferredEvent ---");
        // Setup
        UUID fromWalletId = UUID.randomUUID();
        UUID toWalletId = UUID.randomUUID();
        TransferredEvent event = new TransferredEvent(UUID.randomUUID(), fromWalletId, toWalletId, BigDecimal.TEN, BigDecimal.TEN, BigDecimal.TEN, "", OffsetDateTime.now(), "USD");
        ConsumerRecord<String, Object> record = createRecord(fromWalletId.toString(), event);
        Acknowledgment ack = mock(Acknowledgment.class);
        doNothing().when(cacheService).invalidateCache(any(UUID.class));

        // Execução
        cacheInvalidationConsumer.processEvent(record, ack);

        // Verificação
        verify(cacheService, times(1)).invalidateCache(fromWalletId);
        verify(cacheService, times(1)).invalidateCache(toWalletId);
        verify(ack, times(1)).acknowledge();
    }

    // ===================================================================
    // =================== TESTES DE CENÁRIOS DE FALHA ===================
    // ===================================================================

    @Test
    @DisplayName("Deve propagar exceção se o CacheService falhar")
    void shouldPropagateExceptionWhenCacheServiceFails() {
        logger.info("--- Teste: Propagação de exceção do CacheService ---");
        // Setup
        UUID walletId = UUID.randomUUID();
        DepositedEvent event = new DepositedEvent(UUID.randomUUID(), walletId, BigDecimal.TEN, BigDecimal.TEN, "", OffsetDateTime.now());
        ConsumerRecord<String, Object> record = createRecord(walletId.toString(), event);
        Acknowledgment ack = mock(Acknowledgment.class);
        doThrow(new RuntimeException("Redis is down")).when(cacheService).invalidateCache(walletId);

        // Execução e Verificação
        assertThrows(RuntimeException.class, () -> {
            cacheInvalidationConsumer.processEvent(record, ack);
        });

        // Garante que o acknowledge NUNCA é chamado se uma exceção ocorrer
        verify(ack, never()).acknowledge();
    }

    @Test
    @DisplayName("Deve propagar exceção para TransferredEvent se a segunda invalidação falhar")
    void shouldPropagateExceptionIfSecondInvalidationFailsForTransferredEvent() {
        logger.info("--- Teste: Falha parcial no TransferredEvent ---");
        // Setup
        UUID fromWalletId = UUID.randomUUID();
        UUID toWalletId = UUID.randomUUID();
        TransferredEvent event = new TransferredEvent(UUID.randomUUID(), fromWalletId, toWalletId, BigDecimal.TEN, BigDecimal.TEN, BigDecimal.TEN, "", OffsetDateTime.now(), "USD");
        ConsumerRecord<String, Object> record = createRecord(fromWalletId.toString(), event);
        Acknowledgment ack = mock(Acknowledgment.class);

        // A primeira chamada funciona, a segunda falha
        doNothing().when(cacheService).invalidateCache(fromWalletId);
        doThrow(new RuntimeException("Redis is down")).when(cacheService).invalidateCache(toWalletId);

        // Execução e Verificação
        assertThrows(RuntimeException.class, () -> {
            cacheInvalidationConsumer.processEvent(record, ack);
        });

        // Verifica que ambas as chamadas foram tentadas
        verify(cacheService, times(1)).invalidateCache(fromWalletId);
        verify(cacheService, times(1)).invalidateCache(toWalletId);
        verify(ack, never()).acknowledge();
    }

    // ===================================================================
    // =================== TESTES DE CENÁRIOS DE BORDA ===================
    // ===================================================================

    @Test
    @DisplayName("Deve Apenas Confirmar (Acknowledge) se o Evento for Nulo")
    void shouldAcknowledgeAndStopWhenEventIsNull() {
        logger.info("--- Teste: Evento nulo ---");
        // Setup
        ConsumerRecord<String, Object> record = createRecord("some-key", null);
        Acknowledgment ack = mock(Acknowledgment.class);

        // Execução
        cacheInvalidationConsumer.processEvent(record, ack);

        // Verificação
        verifyNoInteractions(cacheService); // Nenhum serviço de negócio deve ser chamado
        verify(ack, times(1)).acknowledge();
    }

    @Test
    @DisplayName("Deve Apenas Confirmar (Acknowledge) para Tipo de Evento Não Suportado")
    void shouldAcknowledgeAndStopForUnsupportedEventType() {
        logger.info("--- Teste: Evento não suportado ---");
        // Setup
        ConsumerRecord<String, Object> record = createRecord("some-key", new Object()); // Um objeto qualquer
        Acknowledgment ack = mock(Acknowledgment.class);

        // Execução
        cacheInvalidationConsumer.processEvent(record, ack);

        // Verificação
        verifyNoInteractions(cacheService);
        verify(ack, times(1)).acknowledge();
    }
}
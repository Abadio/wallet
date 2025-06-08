package com.recargapay.wallet.query.service;

import com.recargapay.wallet.common.event.DepositedEvent;
import com.recargapay.wallet.common.event.TransferredEvent;
import com.recargapay.wallet.common.event.WithdrawnEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class CacheInvalidationConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(CacheInvalidationConsumer.class);

    private final CacheService cacheService;
    private final Map<Class<?>, EventProcessor> eventProcessors;

    public CacheInvalidationConsumer(CacheService cacheService) {
        this.cacheService = cacheService;
        this.eventProcessors = initializeEventProcessors();
    }

    private Map<Class<?>, EventProcessor> initializeEventProcessors() {
        Map<Class<?>, EventProcessor> processors = new HashMap<>();
        processors.put(DepositedEvent.class, new DepositedEventProcessor());
        processors.put(WithdrawnEvent.class, new WithdrawnEventProcessor());
        processors.put(TransferredEvent.class, new TransferredEventProcessor());
        return processors;
    }

    @KafkaListener(
            topics = "wallet-events",
            groupId = "${spring.kafka.consumer.main.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeEvent(ConsumerRecord<String, Object> record, Acknowledgment acknowledgment) {
        // Apenas delega para o método com a lógica, tornando-o testável
        processEvent(record, acknowledgment);
    }

    // Método público com a lógica real para ser testado
    public void processEvent(ConsumerRecord<String, Object> record, Acknowledgment acknowledgment) {
        LOGGER.info("Processing ConsumerRecord: topic={}, key={}, offset={}",
                record.topic(), record.key(), record.offset());
        LOGGER.debug("Record value: {}", record.value());

        try {
            Object event = record.value();
            if (event == null) {
                LOGGER.error("Received null event: topic={}, key={}, offset={}", record.topic(), record.key(), record.offset());
                acknowledgment.acknowledge();
                return;
            }

            EventProcessor processor = eventProcessors.get(event.getClass());
            if (processor == null) {
                LOGGER.warn("Unsupported event type: {}, topic={}, key={}, offset={}",
                        event.getClass().getName(), record.topic(), record.key(), record.offset());
                acknowledgment.acknowledge();
                return;
            }

            processor.process(event);
            acknowledgment.acknowledge();
        } catch (Exception e) {
            LOGGER.error("Error processing event: topic={}, key={}, offset={}, value={}",
                    record.topic(), record.key(), record.offset(), record.value(), e);
            // Re-lança a exceção para que o ErrorHandler do Kafka a capture e envie para o DLT.
            throw e;
        }
    }

    private interface EventProcessor {
        void process(Object event);
    }

    private class DepositedEventProcessor implements EventProcessor {
        @Override
        public void process(Object event) {
            DepositedEvent depositedEvent = (DepositedEvent) event;
            UUID walletId = depositedEvent.getWalletId();
            LOGGER.info("Invalidating cache for DepositedEvent: walletId={}", walletId);
            cacheService.invalidateCache(walletId);
            LOGGER.info("Cache invalidated for DepositedEvent: walletId={}", walletId);
        }
    }

    private class WithdrawnEventProcessor implements EventProcessor {
        @Override
        public void process(Object event) {
            WithdrawnEvent withdrawnEvent = (WithdrawnEvent) event;
            UUID walletId = withdrawnEvent.getWalletId();
            LOGGER.info("Invalidating cache for WithdrawnEvent: walletId={}", walletId);
            cacheService.invalidateCache(walletId);
            LOGGER.info("Cache invalidated for WithdrawnEvent: walletId={}", walletId);
        }
    }

    private class TransferredEventProcessor implements EventProcessor {
        @Override
        public void process(Object event) {
            TransferredEvent transferredEvent = (TransferredEvent) event;
            UUID fromWalletId = transferredEvent.getFromWalletId();
            UUID toWalletId = transferredEvent.getToWalletId();
            LOGGER.info("Invalidating cache for TransferredEvent: fromWalletId={}, toWalletId={}", fromWalletId, toWalletId);

            // Lógica original de invalidação dupla
            cacheService.invalidateCache(fromWalletId);
            cacheService.invalidateCache(toWalletId);

            LOGGER.info("Cache invalidation processed for TransferredEvent: fromWalletId={}, toWalletId={}", fromWalletId, toWalletId);
        }
    }
}
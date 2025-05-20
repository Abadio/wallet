package com.recargapay.wallet.consumer.service;

import com.recargapay.wallet.common.event.DepositedEvent;
import com.recargapay.wallet.common.event.WithdrawnEvent;
import com.recargapay.wallet.common.event.TransferredEvent;
import com.recargapay.wallet.consumer.document.DailyBalanceDocument;
import com.recargapay.wallet.consumer.document.TransactionHistoryDocument;
import com.recargapay.wallet.consumer.document.WalletBalanceDocument;
import com.recargapay.wallet.consumer.model.User;
import com.recargapay.wallet.consumer.model.Wallet;
import com.recargapay.wallet.consumer.repository.DailyBalanceMongoRepository;
import com.recargapay.wallet.consumer.repository.TransactionHistoryMongoRepository;
import com.recargapay.wallet.consumer.repository.UserRepository;
import com.recargapay.wallet.consumer.repository.WalletBalanceMongoRepository;
import com.recargapay.wallet.consumer.repository.WalletRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;

/**
 * Consumes events from Kafka topics and updates wallet-related data in MongoDB and JPA repositories.
 * Uses a strategy pattern to process different event types (Deposited, Withdrawn, Transferred).
 */
@Service
public class WalletEventConsumer {
    private static final Logger logger = LoggerFactory.getLogger(WalletEventConsumer.class);

    private final WalletBalanceMongoRepository walletBalanceMongoRepository;
    private final TransactionHistoryMongoRepository transactionHistoryMongoRepository;
    private final DailyBalanceMongoRepository dailyBalanceMongoRepository;
    private final UserRepository userRepository;
    private final WalletRepository walletRepository;
    private final Map<Class<?>, EventProcessor> eventProcessors;

    /**
     * Constructs a WalletEventConsumer with required repositories and initializes event processors.
     *
     * @param walletBalanceMongoRepository Repository for wallet balance documents
     * @param transactionHistoryMongoRepository Repository for transaction history documents
     * @param dailyBalanceMongoRepository Repository for daily balance documents
     * @param userRepository Repository for user entities
     * @param walletRepository Repository for wallet entities
     */
    public WalletEventConsumer(
            WalletBalanceMongoRepository walletBalanceMongoRepository,
            TransactionHistoryMongoRepository transactionHistoryMongoRepository,
            DailyBalanceMongoRepository dailyBalanceMongoRepository,
            UserRepository userRepository,
            WalletRepository walletRepository) {
        this.walletBalanceMongoRepository = walletBalanceMongoRepository;
        this.transactionHistoryMongoRepository = transactionHistoryMongoRepository;
        this.dailyBalanceMongoRepository = dailyBalanceMongoRepository;
        this.userRepository = userRepository;
        this.walletRepository = walletRepository;
        this.eventProcessors = initializeEventProcessors();
        logger.info("WalletEventConsumer initialized");
    }

    /**
     * Initializes the map of event processors for each supported event type.
     *
     * @return Map of event classes to their respective processors
     */
    private Map<Class<?>, EventProcessor> initializeEventProcessors() {
        Map<Class<?>, EventProcessor> processors = new HashMap<>();
        processors.put(DepositedEvent.class, new DepositedEventProcessor());
        processors.put(WithdrawnEvent.class, new WithdrawnEventProcessor());
        processors.put(TransferredEvent.class, new TransferredEventProcessor());
        return processors;
    }

    /**
     * Consumes events from the 'wallet-events' Kafka topic and delegates processing to the appropriate event processor.
     *
     * @param record The Kafka ConsumerRecord containing the event
     * @param acknowledgment Kafka acknowledgment for manual commit
     */
    @KafkaListener(
            topics = "wallet-events",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeEvent(ConsumerRecord<String, Object> record, Acknowledgment acknowledgment) {
        logger.info("Processing ConsumerRecord: topic={}, key={}, offset={}",
                record.topic(), record.key(), record.offset());
        logger.debug("Record value: {}", record.value());

        try {
            Object event = record.value();
            if (event == null) {
                logger.error("Received null event: topic={}, key={}, offset={}", record.topic(), record.key(), record.offset());
                acknowledgment.acknowledge();
                return;
            }

            logger.debug("Event type: {}", event.getClass().getName());
            EventProcessor processor = eventProcessors.get(event.getClass());
            if (processor == null) {
                logger.warn("Unsupported event type: {}, topic={}, key={}, offset={}",
                        event.getClass().getName(), record.topic(), record.key(), record.offset());
                acknowledgment.acknowledge();
                return;
            }

            processor.process(event, acknowledgment);
        } catch (Exception e) {
            logger.error("Error processing event: topic={}, key={}, offset={}, value={}",
                    record.topic(), record.key(), record.offset(), record.value(), e);
            throw e; // Re-throw to let KafkaErrorHandler handle it
        }
    }

    /**
     * Interface for processing specific event types.
     */
    private interface EventProcessor {
        void process(Object event, Acknowledgment acknowledgment);
    }

    /**
     * Processor for DepositedEvent, handling deposit-related updates.
     */
    private class DepositedEventProcessor implements EventProcessor {
        @Override
        public void process(Object event, Acknowledgment acknowledgment) {
            DepositedEvent depositedEvent = (DepositedEvent) event;
            logger.info("Processing DepositedEvent: walletId={}, transactionId={}",
                    depositedEvent.getWalletId(), depositedEvent.getTransactionId());

            if (isDuplicate(depositedEvent.getTransactionId())) {
                acknowledgment.acknowledge();
                return;
            }

            Wallet wallet = findWallet(depositedEvent.getWalletId());
            User user = findUser(wallet.getUser().getId());
            saveDocuments(depositedEvent, wallet, user, "DEPOSIT", null, null);
            acknowledgment.acknowledge();
        }
    }

    /**
     * Processor for WithdrawnEvent, handling withdrawal-related updates.
     */
    private class WithdrawnEventProcessor implements EventProcessor {
        @Override
        public void process(Object event, Acknowledgment acknowledgment) {
            WithdrawnEvent withdrawnEvent = (WithdrawnEvent) event;
            logger.info("Processing WithdrawnEvent: walletId={}, transactionId={}",
                    withdrawnEvent.getWalletId(), withdrawnEvent.getTransactionId());

            if (isDuplicate(withdrawnEvent.getTransactionId())) {
                acknowledgment.acknowledge();
                return;
            }

            Wallet wallet = findWallet(withdrawnEvent.getWalletId());
            User user = findUser(wallet.getUser().getId());
            saveDocuments(withdrawnEvent, wallet, user, "WITHDRAWAL", null, null);
            acknowledgment.acknowledge();
        }
    }

    /**
     * Processor for TransferredEvent, handling transfer-related updates based on transactionType.
     */
    private class TransferredEventProcessor implements EventProcessor {
        @Override
        public void process(Object event, Acknowledgment acknowledgment) {
            TransferredEvent transferredEvent = (TransferredEvent) event;
            logger.info("Processing TransferredEvent: fromWalletId={}, toWalletId={}, transactionId={}, transactionType={}",
                    transferredEvent.getFromWalletId(), transferredEvent.getToWalletId(),
                    transferredEvent.getTransactionId(), transferredEvent.getTransactionType());

            if (isDuplicate(transferredEvent.getTransactionId())) {
                logger.warn("Skipping duplicate TransferredEvent: transactionId={}", transferredEvent.getTransactionId());
                acknowledgment.acknowledge();
                return;
            }

            String transactionType = transferredEvent.getTransactionType();
            UUID walletId = transactionType.equals("TRANSFER_SENT") ?
                    transferredEvent.getFromWalletId() : transferredEvent.getToWalletId();
            UUID relatedWalletId = transactionType.equals("TRANSFER_SENT") ?
                    transferredEvent.getToWalletId() : transferredEvent.getFromWalletId();

            Wallet wallet = findWallet(walletId);
            User user = findUser(wallet.getUser().getId());
            User relatedUser = findUser(findWallet(relatedWalletId).getUser().getId());
            String relatedUsername = relatedUser.getUsername();

            saveDocuments(transferredEvent, wallet, user, transactionType, relatedWalletId, relatedUsername);
            logger.info("Successfully saved {} for transactionId={}", transactionType, transferredEvent.getTransactionId());
            acknowledgment.acknowledge();
        }
    }

    /**
     * Checks if a transaction is a duplicate based on its ID.
     *
     * @param transactionId The transaction ID to check
     * @return true if the transaction exists, false otherwise
     */
    private boolean isDuplicate(UUID transactionId) {
        logger.trace("Checking for duplicate transaction: transactionId={}", transactionId);
        boolean exists = transactionHistoryMongoRepository.existsById(transactionId);
        if (exists) {
            logger.warn("Duplicate transaction ignored: transactionId={}", transactionId);
        }
        return exists;
    }

    /**
     * Saves wallet balance, transaction history, and daily balance documents for an event.
     *
     * @param event The event to process
     * @param wallet The associated wallet
     * @param user The associated user
     * @param transactionType The type of transaction (e.g., DEPOSIT, WITHDRAWAL, TRANSFER_SENT, TRANSFER_RECEIVED)
     * @param relatedWalletId ID of the related wallet (for transfers)
     * @param relatedUsername Username of the related user (for transfers)
     */
    private void saveDocuments(Object event, Wallet wallet, User user, String transactionType,
                               UUID relatedWalletId, String relatedUsername) {
        UUID walletId;
        UUID transactionId;
        BigDecimal amount;
        BigDecimal balanceAfter;
        String description;
        OffsetDateTime createdAt;

        if (event instanceof DepositedEvent e) {
            walletId = e.getWalletId();
            transactionId = e.getTransactionId();
            amount = e.getAmount();
            balanceAfter = e.getBalanceAfter();
            description = e.getDescription();
            createdAt = e.getCreatedAt();
        } else if (event instanceof WithdrawnEvent e) {
            walletId = e.getWalletId();
            transactionId = e.getTransactionId();
            amount = e.getAmount().negate();
            balanceAfter = e.getBalanceAfter();
            description = e.getDescription();
            createdAt = e.getCreatedAt();
        } else if (event instanceof TransferredEvent e) {
            walletId = transactionType.equals("TRANSFER_SENT") ? e.getFromWalletId() : e.getToWalletId();
            transactionId = e.getTransactionId();
            amount = transactionType.equals("TRANSFER_SENT") ? e.getAmount().negate() : e.getAmount();
            balanceAfter = transactionType.equals("TRANSFER_SENT") ? e.getFromBalanceAfter() : e.getToBalanceAfter();
            description = e.getDescription();
            createdAt = e.getCreatedAt();
        } else {
            throw new IllegalArgumentException("Unsupported event type: " + event.getClass().getName());
        }

        logger.trace("Preparing to save WalletBalanceDocument: walletId={}, balance={}", walletId, balanceAfter);
        WalletBalanceDocument balanceDocument = new WalletBalanceDocument(
                walletId, user.getId(), user.getUsername(), wallet.getCurrency(),
                balanceAfter, transactionId, createdAt);
        walletBalanceMongoRepository.save(balanceDocument);
        logger.trace("Saved WalletBalanceDocument: walletId={}", walletId);

        logger.trace("Preparing to save TransactionHistoryDocument: transactionId={}, type={}", transactionId, transactionType);
        TransactionHistoryDocument historyDocument = new TransactionHistoryDocument(
                transactionId, walletId, user.getId(), user.getUsername(),
                transactionType, amount, balanceAfter, relatedWalletId,
                relatedUsername, description, createdAt);
        transactionHistoryMongoRepository.save(historyDocument);
        logger.trace("Saved TransactionHistoryDocument: transactionId={}", transactionId);

        String date = createdAt.toLocalDate().toString();
        logger.trace("Preparing to save DailyBalanceDocument: walletId={}, date={}", walletId, date);
        DailyBalanceDocument dailyBalanceDocument = new DailyBalanceDocument(
                walletId, date, user.getId(), user.getUsername(),
                balanceAfter, transactionId, createdAt);
        dailyBalanceMongoRepository.save(dailyBalanceDocument);
        logger.trace("Saved DailyBalanceDocument: walletId={}, date={}", walletId, date);
    }

    /**
     * Finds a wallet by its ID, throwing an exception if not found.
     *
     * @param walletId The wallet ID
     * @return The wallet entity
     * @throws NoSuchElementException if the wallet is not found
     */
    private Wallet findWallet(UUID walletId) {
        logger.debug("Finding wallet: walletId={}", walletId);
        return walletRepository.findById(walletId)
                .orElseThrow(() -> {
                    logger.error("Wallet not found: walletId={}", walletId);
                    return new NoSuchElementException("Wallet not found: " + walletId);
                });
    }

    /**
     * Finds a user by their ID, throwing an exception if not found.
     *
     * @param userId The user ID
     * @return The user entity
     * @throws NoSuchElementException if the user is not found
     */
    private User findUser(UUID userId) {
        logger.debug("Finding user: userId={}", userId);
        return userRepository.findById(userId)
                .orElseThrow(() -> {
                    logger.error("User not found: userId={}", userId);
                    return new NoSuchElementException("User not found: " + userId);
                });
    }
}
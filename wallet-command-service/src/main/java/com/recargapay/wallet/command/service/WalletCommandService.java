package com.recargapay.wallet.command.service;

import com.recargapay.wallet.command.dto.DepositCommand;
import com.recargapay.wallet.command.dto.TransferCommand;
import com.recargapay.wallet.command.dto.WithdrawCommand;
import com.recargapay.wallet.command.model.Event;
import com.recargapay.wallet.command.model.Transaction;
import com.recargapay.wallet.command.model.WalletBalance;
import com.recargapay.wallet.command.repository.EventRepository;
import com.recargapay.wallet.command.repository.TransactionRepository;
import com.recargapay.wallet.command.repository.WalletBalanceRepository;
import com.recargapay.wallet.command.repository.WalletRepository;
import com.recargapay.wallet.common.event.DepositedEvent;
import com.recargapay.wallet.common.event.TransferredEvent;
import com.recargapay.wallet.common.event.WithdrawnEvent;
import jakarta.transaction.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.NoSuchElementException;
import java.util.UUID;

/**
 * Service for handling wallet-related commands (deposit, withdraw, transfer).
 * Publishes events to Kafka for further processing by consumers.
 */
@Service
public class WalletCommandService {
    private static final Logger logger = LoggerFactory.getLogger(WalletCommandService.class);

    private final WalletRepository walletRepository;
    private final WalletBalanceRepository walletBalanceRepository;
    private final TransactionRepository transactionRepository;
    private final EventRepository eventRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public WalletCommandService(
            WalletRepository walletRepository,
            WalletBalanceRepository walletBalanceRepository,
            TransactionRepository transactionRepository,
            EventRepository eventRepository,
            KafkaTemplate<String, Object> kafkaTemplate) {
        this.walletRepository = walletRepository;
        this.walletBalanceRepository = walletBalanceRepository;
        this.transactionRepository = transactionRepository;
        this.eventRepository = eventRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Transactional
    public void handle(DepositCommand command) {
        logger.info("Processing DepositCommand: walletId={}, amount={}, description={}",
                command.getWalletId(), command.getAmount(), command.getDescription());

        if (command.getAmount().compareTo(BigDecimal.ZERO) <= 0) {
            logger.error("Invalid amount for DepositCommand: amount={}", command.getAmount());
            throw new IllegalArgumentException("Amount must be positive");
        }

        WalletBalance walletBalance = walletBalanceRepository.findByWalletIdWithLock(command.getWalletId())
                .orElseThrow(() -> {
                    logger.error("Wallet balance not found for walletId: {}", command.getWalletId());
                    return new NoSuchElementException("Wallet balance not found for walletId: " + command.getWalletId());
                });

        BigDecimal newBalance = walletBalance.getBalance().add(command.getAmount());
        logger.debug("Calculated new balance: walletId={}, newBalance={}", command.getWalletId(), newBalance);

        Transaction transaction = new Transaction();
        transaction.setWallet(walletRepository.getReferenceById(command.getWalletId()));
        transaction.setTransactionType("DEPOSIT");
        transaction.setAmount(command.getAmount());
        transaction.setBalanceAfter(newBalance);
        transaction.setDescription(command.getDescription());
        transaction.setCreatedAt(command.getCreatedAt());
        transactionRepository.save(transaction);
        logger.debug("Saved transaction: transactionId={}, walletId={}", transaction.getId(), command.getWalletId());

        walletBalance.setBalance(newBalance);
        walletBalance.setLastTransactionId(transaction.getId());
        walletBalance.setUpdatedAt(command.getCreatedAt());
        walletBalanceRepository.save(walletBalance);
        logger.debug("Updated wallet balance: walletId={}, balance={}", command.getWalletId(), newBalance);

        DepositedEvent event = new DepositedEvent(
                transaction.getId(),
                command.getWalletId(),
                command.getAmount(),
                newBalance,
                command.getDescription(),
                command.getCreatedAt()
        );
        publishEvent(command.getWalletId(), "DepositedEvent", event);
        logger.info("Successfully processed DepositCommand: walletId={}, transactionId={}",
                command.getWalletId(), transaction.getId());
    }

    @Transactional
    public void handle(WithdrawCommand command) {
        logger.info("Processing WithdrawCommand: walletId={}, amount={}, description={}",
                command.getWalletId(), command.getAmount(), command.getDescription());

        if (command.getAmount().compareTo(BigDecimal.ZERO) <= 0) {
            logger.error("Invalid amount for WithdrawCommand: amount={}", command.getAmount());
            throw new IllegalArgumentException("Amount must be positive");
        }

        WalletBalance walletBalance = walletBalanceRepository.findByWalletIdWithLock(command.getWalletId())
                .orElseThrow(() -> {
                    logger.error("Wallet balance not found for walletId: {}", command.getWalletId());
                    return new NoSuchElementException("Wallet balance not found for walletId: " + command.getWalletId());
                });

        BigDecimal newBalance = walletBalance.getBalance().subtract(command.getAmount());
        if (newBalance.compareTo(BigDecimal.ZERO) < 0) {
            logger.error("Insufficient balance for walletId: {}, currentBalance={}, requestedAmount={}",
                    command.getWalletId(), walletBalance.getBalance(), command.getAmount());
            throw new IllegalStateException("Insufficient balance for walletId: " + command.getWalletId());
        }

        Transaction transaction = new Transaction();
        transaction.setWallet(walletRepository.getReferenceById(command.getWalletId()));
        transaction.setTransactionType("WITHDRAWAL");
        transaction.setAmount(command.getAmount().negate());
        transaction.setBalanceAfter(newBalance);
        transaction.setDescription(command.getDescription());
        transaction.setCreatedAt(command.getCreatedAt());
        transactionRepository.save(transaction);
        logger.debug("Saved transaction: transactionId={}, walletId={}", transaction.getId(), command.getWalletId());

        walletBalance.setBalance(newBalance);
        walletBalance.setLastTransactionId(transaction.getId());
        walletBalance.setUpdatedAt(command.getCreatedAt());
        walletBalanceRepository.save(walletBalance);
        logger.debug("Updated wallet balance: walletId={}, balance={}", command.getWalletId(), newBalance);

        WithdrawnEvent event = new WithdrawnEvent(
                transaction.getId(),
                command.getWalletId(),
                command.getAmount(),
                newBalance,
                command.getDescription(),
                command.getCreatedAt()
        );
        publishEvent(command.getWalletId(), "WithdrawnEvent", event);
        logger.info("Successfully processed WithdrawCommand: walletId={}, transactionId={}",
                command.getWalletId(), transaction.getId());
    }

    @Transactional
    public void handle(TransferCommand command) {
        logger.info("Processing TransferCommand: fromWalletId={}, toWalletId={}, amount={}, description={}",
                command.getFromWalletId(), command.getToWalletId(), command.getAmount(), command.getDescription());

        if (command.getAmount().compareTo(BigDecimal.ZERO) <= 0) {
            logger.error("Invalid amount for TransferCommand: amount={}", command.getAmount());
            throw new IllegalArgumentException("Amount must be positive");
        }
        if (command.getFromWalletId().equals(command.getToWalletId())) {
            logger.error("Cannot transfer to the same wallet: walletId={}", command.getFromWalletId());
            throw new IllegalArgumentException("Cannot transfer to the same wallet");
        }

        // Lock wallets in a consistent order to prevent deadlocks
        UUID firstWalletId = command.getFromWalletId().compareTo(command.getToWalletId()) < 0 ? command.getFromWalletId() : command.getToWalletId();
        UUID secondWalletId = command.getFromWalletId().compareTo(command.getToWalletId()) < 0 ? command.getToWalletId() : command.getFromWalletId();

        WalletBalance firstBalance = walletBalanceRepository.findByWalletIdWithLock(firstWalletId)
                .orElseThrow(() -> {
                    logger.error("Wallet balance not found for walletId: {}", firstWalletId);
                    return new NoSuchElementException("Wallet balance not found for walletId: " + firstWalletId);
                });
        WalletBalance secondBalance = walletBalanceRepository.findByWalletIdWithLock(secondWalletId)
                .orElseThrow(() -> {
                    logger.error("Wallet balance not found for walletId: {}", secondWalletId);
                    return new NoSuchElementException("Wallet balance not found for walletId: " + secondWalletId);
                });

        WalletBalance fromBalance = firstWalletId.equals(command.getFromWalletId()) ? firstBalance : secondBalance;
        WalletBalance toBalance = firstWalletId.equals(command.getToWalletId()) ? firstBalance : secondBalance;

        BigDecimal newFromBalance = fromBalance.getBalance().subtract(command.getAmount());
        if (newFromBalance.compareTo(BigDecimal.ZERO) < 0) {
            logger.error("Insufficient balance for walletId: {}, currentBalance={}, requestedAmount={}",
                    command.getFromWalletId(), fromBalance.getBalance(), command.getAmount());
            throw new IllegalStateException("Insufficient balance for walletId: " + command.getFromWalletId());
        }
        BigDecimal newToBalance = toBalance.getBalance().add(command.getAmount());

        Transaction sentTransaction = new Transaction();
        sentTransaction.setWallet(walletRepository.getReferenceById(command.getFromWalletId()));
        sentTransaction.setTransactionType("TRANSFER_SENT");
        sentTransaction.setAmount(command.getAmount().negate());
        sentTransaction.setBalanceAfter(newFromBalance);
        sentTransaction.setRelatedWallet(walletRepository.getReferenceById(command.getToWalletId()));
        sentTransaction.setDescription(command.getDescription());
        sentTransaction.setCreatedAt(command.getCreatedAt());
        transactionRepository.save(sentTransaction);
        logger.debug("Saved sent transaction: transactionId={}, fromWalletId={}",
                sentTransaction.getId(), command.getFromWalletId());

        Transaction receivedTransaction = new Transaction();
        receivedTransaction.setWallet(walletRepository.getReferenceById(command.getToWalletId()));
        receivedTransaction.setTransactionType("TRANSFER_RECEIVED");
        receivedTransaction.setAmount(command.getAmount());
        receivedTransaction.setBalanceAfter(newToBalance);
        receivedTransaction.setRelatedWallet(walletRepository.getReferenceById(command.getFromWalletId()));
        receivedTransaction.setDescription(command.getDescription());
        receivedTransaction.setCreatedAt(command.getCreatedAt());
        transactionRepository.save(receivedTransaction);
        logger.debug("Saved received transaction: transactionId={}, toWalletId={}",
                receivedTransaction.getId(), command.getToWalletId());

        fromBalance.setBalance(newFromBalance);
        fromBalance.setLastTransactionId(sentTransaction.getId());
        fromBalance.setUpdatedAt(command.getCreatedAt());
        walletBalanceRepository.save(fromBalance);
        logger.debug("Updated from wallet balance: walletId={}, balance={}",
                command.getFromWalletId(), newFromBalance);

        toBalance.setBalance(newToBalance);
        toBalance.setLastTransactionId(receivedTransaction.getId());
        toBalance.setUpdatedAt(command.getCreatedAt());
        walletBalanceRepository.save(toBalance);
        logger.debug("Updated to wallet balance: walletId={}, balance={}",
                command.getToWalletId(), newToBalance);

        // Publish event for TRANSFER_SENT
        TransferredEvent sentEvent = new TransferredEvent(
                sentTransaction.getId(),
                command.getFromWalletId(),
                command.getToWalletId(),
                command.getAmount(),
                newFromBalance,
                newToBalance,
                command.getDescription(),
                command.getCreatedAt(),
                "TRANSFER_SENT"
        );
        publishEvent(command.getFromWalletId(), "TransferredEvent", sentEvent);
        logger.info("Published TRANSFER_SENT event: transactionId={}, fromWalletId={}",
                sentTransaction.getId(), command.getFromWalletId());

        // Publish event for TRANSFER_RECEIVED
        TransferredEvent receivedEvent = new TransferredEvent(
                receivedTransaction.getId(),
                command.getFromWalletId(),
                command.getToWalletId(),
                command.getAmount(),
                newFromBalance,
                newToBalance,
                command.getDescription(),
                command.getCreatedAt(),
                "TRANSFER_RECEIVED"
        );
        publishEvent(command.getToWalletId(), "TransferredEvent", receivedEvent);
        logger.info("Published TRANSFER_RECEIVED event: transactionId={}, toWalletId={}",
                receivedTransaction.getId(), command.getToWalletId());

        logger.info("Successfully processed TransferCommand: sentTransactionId={}, receivedTransactionId={}, fromWalletId={}, toWalletId={}",
                sentTransaction.getId(), receivedTransaction.getId(), command.getFromWalletId(), command.getToWalletId());
    }

    private void publishEvent(UUID aggregateId, String eventType, Object event) {
        try {
            logger.info("Publishing event: type={}, aggregateId={}", eventType, aggregateId);

            Event eventEntity = new Event();
            eventEntity.setAggregateId(aggregateId);
            eventEntity.setEventType(eventType);
            eventEntity.setEventData(event.toString()); // Store event as string for auditing
            eventRepository.save(eventEntity);
            logger.debug("Saved event entity: eventId={}, type={}, aggregateId={}",
                    eventEntity.getId(), eventType, aggregateId);

            kafkaTemplate.send("wallet-events", eventEntity.getId().toString(), event);
            logger.info("Successfully published event: type={}, eventId={}, aggregateId={}",
                    eventType, eventEntity.getId(), aggregateId);
        } catch (Exception e) {
            logger.error("Failed to publish event: type={}, aggregateId={}, error={}",
                    eventType, aggregateId, e.getMessage(), e);
            throw new RuntimeException("Failed to publish event: " + eventType, e);
        }
    }
}
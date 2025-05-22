package com.recargapay.wallet.command.service;

import com.recargapay.wallet.command.dto.DepositCommand;
import com.recargapay.wallet.command.dto.TransferCommand;
import com.recargapay.wallet.command.dto.WithdrawCommand;
import com.recargapay.wallet.command.model.Event;
import com.recargapay.wallet.command.model.Transaction;
import com.recargapay.wallet.command.model.User;
import com.recargapay.wallet.command.model.Wallet;
import com.recargapay.wallet.command.model.WalletBalance;
import com.recargapay.wallet.command.repository.EventRepository;
import com.recargapay.wallet.command.repository.TransactionRepository;
import com.recargapay.wallet.command.repository.UserRepository;
import com.recargapay.wallet.command.repository.WalletBalanceRepository;
import com.recargapay.wallet.command.repository.WalletRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.NoSuchElementException;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for WalletCommandService, validating deposit, withdrawal, and transfer operations
 * using a real H2 database and embedded Kafka. Wallets are initialized with zero balance and
 * lastTransactionId as UUID(0L, 0L) to indicate no prior transactions. Non-zero initial balances
 * are achieved through explicit deposit transactions. Kafka configuration is overridden by
 * TestKafkaConfig for embedded Kafka.
 */
@SpringBootTest
@ActiveProfiles("integration")
@EmbeddedKafka(partitions = 1, topics = {"wallet-events"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class WalletCommandServiceIntegrationTest {

    @Autowired
    private WalletCommandService walletCommandService;

    @Autowired
    private WalletRepository walletRepository;

    @Autowired
    private WalletBalanceRepository walletBalanceRepository;

    @Autowired
    private TransactionRepository transactionRepository;

    @Autowired
    private EventRepository eventRepository;

    @Autowired
    private UserRepository userRepository;

    /**
     * Creates a wallet with a specified ID, initialized with zero balance and lastTransactionId as
     * UUID(0L, 0L) to indicate no prior transactions. This reflects the initial setup state with no
     * user-initiated transactions.
     */
    private Wallet createWallet(UUID walletId) {
        User user = new User();
        user.setUsername("testuser_" + UUID.randomUUID());
        user.setEmail("test_" + UUID.randomUUID() + "@example.com");
        user.setCreatedAt(OffsetDateTime.now());
        user.setUpdatedAt(OffsetDateTime.now());
        user = userRepository.saveAndFlush(user);

        Wallet wallet = new Wallet();
        wallet.setId(walletId);
        wallet.setUser(user);
        wallet.setCurrency("BRL");
        wallet.setCreatedAt(OffsetDateTime.now());
        wallet.setUpdatedAt(OffsetDateTime.now());
        wallet = walletRepository.saveAndFlush(wallet);

        WalletBalance balance = new WalletBalance();
        balance.setWalletId(walletId);
        balance.setBalance(BigDecimal.ZERO);
        balance.setLastTransactionId(new UUID(0L, 0L));
        balance.setUpdatedAt(OffsetDateTime.now());
        walletBalanceRepository.saveAndFlush(balance);

        return wallet;
    }

    @Test
    @Transactional
    void testDeposit_Success() {
        UUID walletId = UUID.randomUUID();
        createWallet(walletId);
        DepositCommand command = new DepositCommand(walletId, new BigDecimal("100.00"), "Test deposit", OffsetDateTime.now());

        walletCommandService.handle(command);

        WalletBalance balance = walletBalanceRepository.findByWalletIdWithLock(walletId).orElseThrow();
        assertEquals(new BigDecimal("100.00"), balance.getBalance());
        assertEquals(1, transactionRepository.count());
        assertEquals(1, eventRepository.count());
    }

    @Test
    void testDeposit_WalletNotFound() {
        UUID walletId = UUID.randomUUID();
        DepositCommand command = new DepositCommand(walletId, new BigDecimal("100.00"), "Test deposit", OffsetDateTime.now());

        assertThrows(NoSuchElementException.class, () -> walletCommandService.handle(command));
        assertEquals(0, walletBalanceRepository.count());
        assertEquals(0, transactionRepository.count());
        assertEquals(0, eventRepository.count());
    }

    @Test
    @Transactional
    void testDeposit_InvalidAmount() {
        UUID walletId = UUID.randomUUID();
        createWallet(walletId);
        DepositCommand command = new DepositCommand(walletId, new BigDecimal("-100.00"), "Test deposit", OffsetDateTime.now());

        assertThrows(IllegalArgumentException.class, () -> walletCommandService.handle(command));
        WalletBalance balance = walletBalanceRepository.findByWalletIdWithLock(walletId).orElseThrow();
        assertEquals(BigDecimal.ZERO, balance.getBalance());
        assertEquals(0, transactionRepository.count());
        assertEquals(0, eventRepository.count());
    }

    @Test
    @Transactional
    void testWithdraw_Success() {
        UUID walletId = UUID.randomUUID();
        createWallet(walletId);
        // Perform initial deposit to set balance
        DepositCommand deposit = new DepositCommand(walletId, new BigDecimal("200.00"), "Initial deposit", OffsetDateTime.now());
        walletCommandService.handle(deposit);

        WithdrawCommand command = new WithdrawCommand(walletId, new BigDecimal("100.00"), "Test withdraw", OffsetDateTime.now());
        walletCommandService.handle(command);

        WalletBalance balance = walletBalanceRepository.findByWalletIdWithLock(walletId).orElseThrow();
        assertEquals(new BigDecimal("100.00"), balance.getBalance());
        assertEquals(2, transactionRepository.count()); // Deposit + withdrawal
        assertEquals(2, eventRepository.count()); // DepositedEvent + WithdrawnEvent
    }

    @Test
    @Transactional
    void testWithdraw_InsufficientBalance() {
        UUID walletId = UUID.randomUUID();
        createWallet(walletId);
        // Perform initial deposit to set balance
        DepositCommand deposit = new DepositCommand(walletId, new BigDecimal("50.00"), "Initial deposit", OffsetDateTime.now());
        walletCommandService.handle(deposit);

        WithdrawCommand command = new WithdrawCommand(walletId, new BigDecimal("100.00"), "Test withdraw", OffsetDateTime.now());
        assertThrows(IllegalStateException.class, () -> walletCommandService.handle(command));

        WalletBalance balance = walletBalanceRepository.findByWalletIdWithLock(walletId).orElseThrow();
        assertEquals(new BigDecimal("50.00"), balance.getBalance());
        assertEquals(1, transactionRepository.count()); // Only initial deposit
        assertEquals(1, eventRepository.count()); // Only DepositedEvent
    }

    @Test
    @Transactional
    void testTransfer_Success() {
        UUID sourceWalletId = UUID.randomUUID();
        UUID targetWalletId = UUID.randomUUID();
        createWallet(sourceWalletId);
        createWallet(targetWalletId);
        // Perform initial deposit to source wallet
        DepositCommand deposit = new DepositCommand(sourceWalletId, new BigDecimal("200.00"), "Initial deposit", OffsetDateTime.now());
        walletCommandService.handle(deposit);

        TransferCommand command = new TransferCommand(sourceWalletId, targetWalletId, new BigDecimal("100.00"), "Test transfer", OffsetDateTime.now());
        walletCommandService.handle(command);

        WalletBalance sourceBalance = walletBalanceRepository.findByWalletIdWithLock(sourceWalletId).orElseThrow();
        WalletBalance targetBalance = walletBalanceRepository.findByWalletIdWithLock(targetWalletId).orElseThrow();
        assertEquals(new BigDecimal("100.00"), sourceBalance.getBalance());
        assertEquals(new BigDecimal("100.00"), targetBalance.getBalance());
        assertEquals(3, transactionRepository.count()); // Deposit + TRANSFER_SENT + TRANSFER_RECEIVED
        assertEquals(3, eventRepository.count()); // DepositedEvent + TRANSFER_SENT + TRANSFER_RECEIVED
    }

    @Test
    @Transactional
    void testTransfer_InsufficientBalance() {
        UUID sourceWalletId = UUID.randomUUID();
        UUID targetWalletId = UUID.randomUUID();
        createWallet(sourceWalletId);
        createWallet(targetWalletId);
        // Perform initial deposit to source wallet
        DepositCommand deposit = new DepositCommand(sourceWalletId, new BigDecimal("50.00"), "Initial deposit", OffsetDateTime.now());
        walletCommandService.handle(deposit);

        TransferCommand command = new TransferCommand(sourceWalletId, targetWalletId, new BigDecimal("100.00"), "Test transfer", OffsetDateTime.now());
        assertThrows(IllegalStateException.class, () -> walletCommandService.handle(command));

        WalletBalance sourceBalance = walletBalanceRepository.findByWalletIdWithLock(sourceWalletId).orElseThrow();
        WalletBalance targetBalance = walletBalanceRepository.findByWalletIdWithLock(targetWalletId).orElseThrow();
        assertEquals(new BigDecimal("50.00"), sourceBalance.getBalance());
        assertEquals(BigDecimal.ZERO, targetBalance.getBalance());
        assertEquals(1, transactionRepository.count()); // Only initial deposit
        assertEquals(1, eventRepository.count()); // Only DepositedEvent
    }

    @Test
    @Transactional
    void testTransfer_SourceWalletNotFound() {
        UUID sourceWalletId = UUID.randomUUID();
        UUID targetWalletId = UUID.randomUUID();
        createWallet(targetWalletId);
        TransferCommand command = new TransferCommand(sourceWalletId, targetWalletId, new BigDecimal("100.00"), "Test transfer", OffsetDateTime.now());

        assertThrows(NoSuchElementException.class, () -> walletCommandService.handle(command));
        WalletBalance targetBalance = walletBalanceRepository.findByWalletIdWithLock(targetWalletId).orElseThrow();
        assertEquals(BigDecimal.ZERO, targetBalance.getBalance());
        assertEquals(0, transactionRepository.count());
        assertEquals(0, eventRepository.count());
    }

    @Test
    @Transactional
    void testTransfer_TargetWalletNotFound() {
        UUID sourceWalletId = UUID.randomUUID();
        UUID targetWalletId = UUID.randomUUID();
        createWallet(sourceWalletId);
        // Perform initial deposit to source wallet
        DepositCommand deposit = new DepositCommand(sourceWalletId, new BigDecimal("200.00"), "Initial deposit", OffsetDateTime.now());
        walletCommandService.handle(deposit);

        TransferCommand command = new TransferCommand(sourceWalletId, targetWalletId, new BigDecimal("100.00"), "Test transfer", OffsetDateTime.now());
        assertThrows(NoSuchElementException.class, () -> walletCommandService.handle(command));

        WalletBalance sourceBalance = walletBalanceRepository.findByWalletIdWithLock(sourceWalletId).orElseThrow();
        assertEquals(new BigDecimal("200.00"), sourceBalance.getBalance());
        assertEquals(1, transactionRepository.count()); // Only initial deposit
        assertEquals(1, eventRepository.count()); // Only DepositedEvent
    }
}
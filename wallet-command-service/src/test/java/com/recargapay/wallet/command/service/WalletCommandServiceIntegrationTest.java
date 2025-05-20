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

    private Wallet createWallet(UUID walletId, BigDecimal initialBalance) {
        System.out.println("Creating wallet with walletId: " + walletId);
        User user = new User("testuser_" + UUID.randomUUID(), "test_" + UUID.randomUUID() + "@example.com");
        userRepository.saveAndFlush(user);

        Wallet wallet = new Wallet(user, "BRL");
        wallet.setId(walletId);
        walletRepository.saveAndFlush(wallet);

        WalletBalance balance = new WalletBalance(walletId);
        balance.setBalance(initialBalance);
        balance.setLastTransactionId(new UUID(0L, 0L));
        balance.setUpdatedAt(OffsetDateTime.now());
        walletBalanceRepository.saveAndFlush(balance);

        return wallet;
    }

    @Test
    @Transactional
    void testDeposit_Success() {
        UUID walletId = UUID.randomUUID();
        System.out.println("TestDeposit_Success using walletId: " + walletId);
        createWallet(walletId, new BigDecimal("200.00"));
        DepositCommand command = new DepositCommand(walletId, new BigDecimal("100.00"), "Test deposit", OffsetDateTime.now());

        walletCommandService.handle(command);

        WalletBalance balance = walletBalanceRepository.findByWalletIdWithLock(walletId).orElseThrow();
        assertEquals(new BigDecimal("300.00"), balance.getBalance());
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
        createWallet(walletId, new BigDecimal("200.00"));
        DepositCommand command = new DepositCommand(walletId, new BigDecimal("-100.00"), "Test deposit", OffsetDateTime.now());

        assertThrows(IllegalArgumentException.class, () -> walletCommandService.handle(command));
        WalletBalance balance = walletBalanceRepository.findByWalletIdWithLock(walletId).orElseThrow();
        assertEquals(new BigDecimal("200.00"), balance.getBalance());
        assertEquals(0, transactionRepository.count());
        assertEquals(0, eventRepository.count());
    }

    @Test
    @Transactional
    void testWithdraw_Success() {
        UUID walletId = UUID.randomUUID();
        createWallet(walletId, new BigDecimal("200.00"));
        WithdrawCommand command = new WithdrawCommand(walletId, new BigDecimal("100.00"), "Test withdraw", OffsetDateTime.now());

        walletCommandService.handle(command);

        WalletBalance balance = walletBalanceRepository.findByWalletIdWithLock(walletId).orElseThrow();
        assertEquals(new BigDecimal("100.00"), balance.getBalance());
        assertEquals(1, transactionRepository.count());
        assertEquals(1, eventRepository.count());
    }

    @Test
    @Transactional
    void testWithdraw_InsufficientBalance() {
        UUID walletId = UUID.randomUUID();
        createWallet(walletId, new BigDecimal("50.00"));
        WithdrawCommand command = new WithdrawCommand(walletId, new BigDecimal("100.00"), "Test withdraw", OffsetDateTime.now());

        assertThrows(IllegalStateException.class, () -> walletCommandService.handle(command));
        WalletBalance balance = walletBalanceRepository.findByWalletIdWithLock(walletId).orElseThrow();
        assertEquals(new BigDecimal("50.00"), balance.getBalance());
        assertEquals(0, transactionRepository.count());
        assertEquals(0, eventRepository.count());
    }

    @Test
    @Transactional
    void testTransfer_Success() {
        UUID sourceWalletId = UUID.randomUUID();
        UUID targetWalletId = UUID.randomUUID();
        createWallet(sourceWalletId, new BigDecimal("200.00"));
        createWallet(targetWalletId, new BigDecimal("50.00"));
        TransferCommand command = new TransferCommand(sourceWalletId, targetWalletId, new BigDecimal("100.00"), "Test transfer", OffsetDateTime.now());

        walletCommandService.handle(command);

        WalletBalance sourceBalance = walletBalanceRepository.findByWalletIdWithLock(sourceWalletId).orElseThrow();
        WalletBalance targetBalance = walletBalanceRepository.findByWalletIdWithLock(targetWalletId).orElseThrow();
        assertEquals(new BigDecimal("100.00"), sourceBalance.getBalance());
        assertEquals(new BigDecimal("150.00"), targetBalance.getBalance());
        assertEquals(2, transactionRepository.count());
        assertEquals(1, eventRepository.count());
    }

    @Test
    @Transactional
    void testTransfer_InsufficientBalance() {
        UUID sourceWalletId = UUID.randomUUID();
        UUID targetWalletId = UUID.randomUUID();
        createWallet(sourceWalletId, new BigDecimal("50.00"));
        createWallet(targetWalletId, new BigDecimal("50.00"));
        TransferCommand command = new TransferCommand(sourceWalletId, targetWalletId, new BigDecimal("100.00"), "Test transfer", OffsetDateTime.now());

        assertThrows(IllegalStateException.class, () -> walletCommandService.handle(command));
        WalletBalance sourceBalance = walletBalanceRepository.findByWalletIdWithLock(sourceWalletId).orElseThrow();
        WalletBalance targetBalance = walletBalanceRepository.findByWalletIdWithLock(targetWalletId).orElseThrow();
        assertEquals(new BigDecimal("50.00"), sourceBalance.getBalance());
        assertEquals(new BigDecimal("50.00"), targetBalance.getBalance());
        assertEquals(0, transactionRepository.count());
        assertEquals(0, eventRepository.count());
    }

    @Test
    @Transactional
    void testTransfer_SourceWalletNotFound() {
        UUID sourceWalletId = UUID.randomUUID();
        UUID targetWalletId = UUID.randomUUID();
        createWallet(targetWalletId, new BigDecimal("50.00"));
        TransferCommand command = new TransferCommand(sourceWalletId, targetWalletId, new BigDecimal("100.00"), "Test transfer", OffsetDateTime.now());

        assertThrows(NoSuchElementException.class, () -> walletCommandService.handle(command));
        WalletBalance targetBalance = walletBalanceRepository.findByWalletIdWithLock(targetWalletId).orElseThrow();
        assertEquals(new BigDecimal("50.00"), targetBalance.getBalance());
        assertEquals(0, transactionRepository.count());
        assertEquals(0, eventRepository.count());
    }

    @Test
    @Transactional
    void testTransfer_TargetWalletNotFound() {
        UUID sourceWalletId = UUID.randomUUID();
        UUID targetWalletId = UUID.randomUUID();
        createWallet(sourceWalletId, new BigDecimal("200.00"));
        TransferCommand command = new TransferCommand(sourceWalletId, targetWalletId, new BigDecimal("100.00"), "Test transfer", OffsetDateTime.now());

        assertThrows(NoSuchElementException.class, () -> walletCommandService.handle(command));
        WalletBalance sourceBalance = walletBalanceRepository.findByWalletIdWithLock(sourceWalletId).orElseThrow();
        assertEquals(new BigDecimal("200.00"), sourceBalance.getBalance());
        assertEquals(0, transactionRepository.count());
        assertEquals(0, eventRepository.count());
    }
}
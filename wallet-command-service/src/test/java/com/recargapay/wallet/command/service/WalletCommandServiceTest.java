package com.recargapay.wallet.command.service;

import com.recargapay.wallet.command.dto.DepositCommand;
import com.recargapay.wallet.command.dto.TransferCommand;
import com.recargapay.wallet.command.dto.WithdrawCommand;
import com.recargapay.wallet.command.model.Event;
import com.recargapay.wallet.command.model.Transaction;
import com.recargapay.wallet.command.model.Wallet;
import com.recargapay.wallet.command.model.WalletBalance;
import com.recargapay.wallet.command.repository.EventRepository;
import com.recargapay.wallet.command.repository.TransactionRepository;
import com.recargapay.wallet.command.repository.WalletBalanceRepository;
import com.recargapay.wallet.command.repository.WalletRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for WalletCommandService, covering deposit, withdrawal, transfer, and error scenarios.
 * Each test is self-contained with its own setup to avoid interference.
 * Tests validate both interactions and resulting state (balance in WalletBalance).
 */
@ExtendWith(MockitoExtension.class)
class WalletCommandServiceTest {

    @Mock
    private WalletRepository walletRepository;

    @Mock
    private WalletBalanceRepository walletBalanceRepository;

    @Mock
    private TransactionRepository transactionRepository;

    @Mock
    private EventRepository walletEventRepository;

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Mock
    private ObjectMapper objectMapper;

    @InjectMocks
    private WalletCommandService walletCommandService;

    /**
     * Tests successful deposit, verifying interactions and new balance.
     */
    @Test
    void testHandleDepositCommand_Success() {
        // Setup
        UUID sourceWalletId = UUID.randomUUID();
        Wallet sourceWallet = new Wallet();
        sourceWallet.setId(sourceWalletId);
        WalletBalance sourceBalance = new WalletBalance();
        sourceBalance.setWalletId(sourceWalletId);
        sourceBalance.setBalance(new BigDecimal("200.00"));
        Transaction dummyTransaction = new Transaction();
        Event dummyEvent = new Event();
        CompletableFuture<SendResult<String, Object>> future = mock(CompletableFuture.class);

        when(walletBalanceRepository.findByWalletIdWithLock(sourceWalletId)).thenReturn(Optional.of(sourceBalance));
        when(walletBalanceRepository.save(any(WalletBalance.class))).thenReturn(sourceBalance);
        when(transactionRepository.save(any())).thenReturn(dummyTransaction);
        when(walletEventRepository.save(any(Event.class))).thenReturn(dummyEvent);
        when(walletRepository.getReferenceById(sourceWalletId)).thenReturn(sourceWallet);
        when(kafkaTemplate.send(eq("wallet-events"), anyString(), any())).thenReturn(future);

        // Prepare command
        BigDecimal depositAmount = new BigDecimal("100.00");
        DepositCommand command = new DepositCommand(
                sourceWalletId,
                depositAmount,
                "Test deposit",
                OffsetDateTime.now()
        );

        // Execute method
        walletCommandService.handle(command);

        // Capture saved WalletBalance
        ArgumentCaptor<WalletBalance> balanceCaptor = ArgumentCaptor.forClass(WalletBalance.class);
        verify(walletBalanceRepository).save(balanceCaptor.capture());
        WalletBalance savedBalance = balanceCaptor.getValue();

        // Validate state
        assertEquals(new BigDecimal("300.00"), savedBalance.getBalance(), "New balance should be initial (200) + deposit (100)");

        // Verify interactions
        verify(walletBalanceRepository, times(1)).findByWalletIdWithLock(sourceWalletId);
        verify(walletBalanceRepository, times(1)).save(any(WalletBalance.class));
        verify(transactionRepository, times(1)).save(any());
        verify(walletEventRepository, times(1)).save(any(Event.class));
        verify(kafkaTemplate, times(1)).send(eq("wallet-events"), anyString(), any());
        verifyNoMoreInteractions(walletBalanceRepository, transactionRepository, walletEventRepository, kafkaTemplate);
    }

    /**
     * Tests deposit failure when source wallet is not found, expecting NoSuchElementException.
     */
    @Test
    void testHandleDepositCommand_WalletNotFound() {
        // Setup
        UUID sourceWalletId = UUID.randomUUID();

        when(walletBalanceRepository.findByWalletIdWithLock(sourceWalletId)).thenReturn(Optional.empty());

        // Prepare command
        DepositCommand command = new DepositCommand(
                sourceWalletId,
                new BigDecimal("100.00"),
                "Test deposit",
                OffsetDateTime.now()
        );

        // Verify exception
        assertThrows(NoSuchElementException.class, () -> walletCommandService.handle(command));

        // Verify interactions
        verify(walletBalanceRepository, times(1)).findByWalletIdWithLock(sourceWalletId);
        verifyNoMoreInteractions(walletBalanceRepository, transactionRepository, walletEventRepository, kafkaTemplate);
    }

    /**
     * Tests successful withdrawal, verifying interactions and new balance.
     */
    @Test
    void testHandleWithdrawCommand_Success() {
        // Setup
        UUID sourceWalletId = UUID.randomUUID();
        Wallet sourceWallet = new Wallet();
        sourceWallet.setId(sourceWalletId);
        WalletBalance sourceBalance = new WalletBalance();
        sourceBalance.setWalletId(sourceWalletId);
        sourceBalance.setBalance(new BigDecimal("200.00"));
        Transaction dummyTransaction = new Transaction();
        Event dummyEvent = new Event();
        CompletableFuture<SendResult<String, Object>> future = mock(CompletableFuture.class);

        when(walletBalanceRepository.findByWalletIdWithLock(sourceWalletId)).thenReturn(Optional.of(sourceBalance));
        when(walletBalanceRepository.save(any(WalletBalance.class))).thenReturn(sourceBalance);
        when(transactionRepository.save(any())).thenReturn(dummyTransaction);
        when(walletEventRepository.save(any(Event.class))).thenReturn(dummyEvent);
        when(walletRepository.getReferenceById(sourceWalletId)).thenReturn(sourceWallet);
        when(kafkaTemplate.send(eq("wallet-events"), anyString(), any())).thenReturn(future);

        // Prepare command
        BigDecimal withdrawAmount = new BigDecimal("100.00");
        WithdrawCommand command = new WithdrawCommand(
                sourceWalletId,
                withdrawAmount,
                "Test withdraw",
                OffsetDateTime.now()
        );

        // Execute method
        walletCommandService.handle(command);

        // Capture saved WalletBalance
        ArgumentCaptor<WalletBalance> balanceCaptor = ArgumentCaptor.forClass(WalletBalance.class);
        verify(walletBalanceRepository).save(balanceCaptor.capture());
        WalletBalance savedBalance = balanceCaptor.getValue();

        // Validate state
        assertEquals(new BigDecimal("100.00"), savedBalance.getBalance(), "New balance should be initial (200) - withdrawal (100)");

        // Verify interactions
        verify(walletBalanceRepository, times(1)).findByWalletIdWithLock(sourceWalletId);
        verify(walletBalanceRepository, times(1)).save(any(WalletBalance.class));
        verify(transactionRepository, times(1)).save(any());
        verify(walletEventRepository, times(1)).save(any(Event.class));
        verify(kafkaTemplate, times(1)).send(eq("wallet-events"), anyString(), any());
        verifyNoMoreInteractions(walletBalanceRepository, transactionRepository, walletEventRepository, kafkaTemplate);
    }

    /**
     * Tests withdrawal failure due to insufficient balance.
     */
    @Test
    void testHandleWithdrawCommand_InsufficientBalance() {
        // Setup
        UUID sourceWalletId = UUID.randomUUID();
        WalletBalance sourceBalance = new WalletBalance();
        sourceBalance.setWalletId(sourceWalletId);
        sourceBalance.setBalance(new BigDecimal("50.00"));

        when(walletBalanceRepository.findByWalletIdWithLock(sourceWalletId)).thenReturn(Optional.of(sourceBalance));

        // Prepare command
        WithdrawCommand command = new WithdrawCommand(
                sourceWalletId,
                new BigDecimal("100.00"),
                "Test withdraw",
                OffsetDateTime.now()
        );

        // Verify exception
        assertThrows(IllegalStateException.class, () -> walletCommandService.handle(command));

        // Verify interactions
        verify(walletBalanceRepository, times(1)).findByWalletIdWithLock(sourceWalletId);
        verifyNoMoreInteractions(walletBalanceRepository, transactionRepository, walletEventRepository, kafkaTemplate);
    }

    /**
     * Tests successful transfer, verifying interactions and new balances for both wallets.
     */
    @Test
    void testHandleTransferCommand_Success() {
        // Setup
        UUID sourceWalletId = UUID.fromString("215b1499-7623-439b-aee8-a1e632e755e4");
        UUID targetWalletId = UUID.fromString("d8e022d2-f675-47dd-910f-333fc636198a");
        Wallet sourceWallet = new Wallet();
        sourceWallet.setId(sourceWalletId);
        Wallet targetWallet = new Wallet();
        targetWallet.setId(targetWalletId);
        WalletBalance sourceBalance = new WalletBalance();
        sourceBalance.setWalletId(sourceWalletId);
        sourceBalance.setBalance(new BigDecimal("200.00"));
        WalletBalance targetBalance = new WalletBalance();
        targetBalance.setWalletId(targetWalletId);
        targetBalance.setBalance(new BigDecimal("50.00"));
        Transaction dummyTransaction = new Transaction();
        Event dummyEvent = new Event();
        CompletableFuture<SendResult<String, Object>> future = mock(CompletableFuture.class);

        when(walletBalanceRepository.findByWalletIdWithLock(sourceWalletId)).thenReturn(Optional.of(sourceBalance));
        when(walletBalanceRepository.findByWalletIdWithLock(targetWalletId)).thenReturn(Optional.of(targetBalance));
        when(walletBalanceRepository.save(any(WalletBalance.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        when(transactionRepository.save(any())).thenReturn(dummyTransaction);
        when(walletEventRepository.save(any(Event.class))).thenReturn(dummyEvent);
        when(walletRepository.getReferenceById(sourceWalletId)).thenReturn(sourceWallet);
        when(walletRepository.getReferenceById(targetWalletId)).thenReturn(targetWallet);
        when(kafkaTemplate.send(eq("wallet-events"), anyString(), any())).thenReturn(future);

        // Prepare command
        BigDecimal transferAmount = new BigDecimal("100.00");
        TransferCommand command = new TransferCommand(
                sourceWalletId,
                targetWalletId,
                transferAmount,
                "Test transfer",
                OffsetDateTime.now()
        );

        // Execute method
        walletCommandService.handle(command);

        // Capture saved WalletBalance instances
        ArgumentCaptor<WalletBalance> balanceCaptor = ArgumentCaptor.forClass(WalletBalance.class);
        verify(walletBalanceRepository, times(2)).save(balanceCaptor.capture());
        WalletBalance savedSourceBalance = balanceCaptor.getAllValues().get(0);
        WalletBalance savedTargetBalance = balanceCaptor.getAllValues().get(1);

        // Validate state
        assertEquals(new BigDecimal("100.00"), savedSourceBalance.getBalance(), "Source balance should be initial (200) - transfer (100)");
        assertEquals(new BigDecimal("150.00"), savedTargetBalance.getBalance(), "Target balance should be initial (50) + transfer (100)");

        // Verify interactions
        verify(walletBalanceRepository, times(1)).findByWalletIdWithLock(sourceWalletId);
        verify(walletBalanceRepository, times(1)).findByWalletIdWithLock(targetWalletId);
        verify(walletBalanceRepository, times(2)).save(any(WalletBalance.class));
        verify(transactionRepository, times(2)).save(any());
        verify(walletEventRepository, times(2)).save(any(Event.class));
        verify(kafkaTemplate, times(2)).send(eq("wallet-events"), anyString(), any());
        verifyNoMoreInteractions(walletBalanceRepository, transactionRepository, walletEventRepository, kafkaTemplate);
    }

    /**
     * Tests transfer failure due to insufficient balance in source wallet.
     */
    @Test
    void testHandleTransferCommand_InsufficientBalance() {
        // Setup
        UUID sourceWalletId = UUID.randomUUID();
        UUID targetWalletId = UUID.randomUUID();
        WalletBalance sourceBalance = new WalletBalance();
        sourceBalance.setWalletId(sourceWalletId);
        sourceBalance.setBalance(new BigDecimal("50.00"));
        WalletBalance targetBalance = new WalletBalance();
        targetBalance.setWalletId(targetWalletId);
        targetBalance.setBalance(new BigDecimal("50.00"));

        when(walletBalanceRepository.findByWalletIdWithLock(sourceWalletId)).thenReturn(Optional.of(sourceBalance));
        when(walletBalanceRepository.findByWalletIdWithLock(targetWalletId)).thenReturn(Optional.of(targetBalance));

        // Prepare command
        TransferCommand command = new TransferCommand(
                sourceWalletId,
                targetWalletId,
                new BigDecimal("100.00"),
                "Test transfer",
                OffsetDateTime.now()
        );

        // Verify exception
        assertThrows(IllegalStateException.class, () -> walletCommandService.handle(command));

        // Verify interactions
        verify(walletBalanceRepository, times(1)).findByWalletIdWithLock(sourceWalletId);
        verify(walletBalanceRepository, times(1)).findByWalletIdWithLock(targetWalletId);
        verifyNoMoreInteractions(walletBalanceRepository, transactionRepository, walletEventRepository, kafkaTemplate);
    }

    /**
     * Tests transfer failure when source wallet is not found.
     */
    @Test
    void testHandleTransferCommand_SourceWalletNotFound() {
        // Setup
        UUID sourceWalletId = UUID.fromString("f221c50f-f61c-49e7-809d-8f1e44197bf2");
        UUID targetWalletId = UUID.fromString("ec578d82-66a6-4176-be32-0a70a3effdd5");
        BigDecimal transferAmount = new BigDecimal("100.00");
        TransferCommand command = new TransferCommand(
                sourceWalletId,
                targetWalletId,
                transferAmount,
                "Test transfer",
                OffsetDateTime.now()
        );

        WalletBalance targetBalance = new WalletBalance();
        targetBalance.setWalletId(targetWalletId);
        targetBalance.setBalance(new BigDecimal("50.00"));

        // Mock wallet balance repository
        when(walletBalanceRepository.findByWalletIdWithLock(sourceWalletId)).thenReturn(Optional.empty());
        when(walletBalanceRepository.findByWalletIdWithLock(targetWalletId)).thenReturn(Optional.of(targetBalance));

        // Execute and verify exception
        assertThrows(NoSuchElementException.class, () -> walletCommandService.handle(command));

        // Verify interactions
        verify(walletBalanceRepository).findByWalletIdWithLock(sourceWalletId);
        verify(walletBalanceRepository).findByWalletIdWithLock(targetWalletId);
        verifyNoMoreInteractions(walletBalanceRepository, walletRepository, transactionRepository, walletEventRepository, kafkaTemplate);
    }

    /**
     * Tests transfer failure when target wallet is not found.
     */
    @Test
    void testHandleTransferCommand_TargetWalletNotFound() {
        // Setup
        UUID sourceWalletId = UUID.fromString("f4c2a626-5b54-4a8a-8954-013eb7b1cb07");
        UUID targetWalletId = UUID.fromString("e729b857-30dd-41c6-a6f7-a38809f5ad9b");
        BigDecimal transferAmount = new BigDecimal("100.00");
        TransferCommand command = new TransferCommand(
                sourceWalletId,
                targetWalletId,
                transferAmount,
                "Test transfer",
                OffsetDateTime.now()
        );

        // Mock wallet balance repository
        when(walletBalanceRepository.findByWalletIdWithLock(targetWalletId)).thenReturn(Optional.empty());

        // Execute and verify exception
        assertThrows(NoSuchElementException.class, () -> walletCommandService.handle(command));

        // Verify interactions
        verify(walletBalanceRepository).findByWalletIdWithLock(targetWalletId);
        verifyNoMoreInteractions(walletBalanceRepository, walletRepository, transactionRepository, walletEventRepository, kafkaTemplate);
    }
}
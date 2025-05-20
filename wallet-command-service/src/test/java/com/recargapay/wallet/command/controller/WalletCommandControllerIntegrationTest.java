package com.recargapay.wallet.command.controller;

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
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("integration")
@EmbeddedKafka(partitions = 1, topics = {"wallet-events"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class WalletCommandControllerIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

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
    void testDeposit_Success() throws Exception {
        // Arrange
        UUID walletId = UUID.randomUUID();
        createWallet(walletId, new BigDecimal("200.00"));

        // Act
        mockMvc.perform(post("/api/command/wallets/{walletId}/deposit", walletId)
                        .param("amount", "100.00")
                        .param("description", "Test deposit")
                        .contentType(MediaType.APPLICATION_FORM_URLENCODED))
                .andExpect(status().isOk());

        // Assert
        WalletBalance balance = walletBalanceRepository.findByWalletIdWithLock(walletId).orElseThrow();
        assertEquals(new BigDecimal("300.00"), balance.getBalance());
        assertEquals(1, transactionRepository.count());
        assertEquals(1, eventRepository.count());
    }

    @Test
    @Transactional
    void testDeposit_WalletNotFound() throws Exception {
        // Arrange
        UUID walletId = UUID.randomUUID();

        // Act
        mockMvc.perform(post("/api/command/wallets/{walletId}/deposit", walletId)
                        .param("amount", "100.00")
                        .param("description", "Test deposit")
                        .contentType(MediaType.APPLICATION_FORM_URLENCODED))
                .andExpect(status().isNotFound());

        // Assert
        assertEquals(0, walletBalanceRepository.count());
        assertEquals(0, transactionRepository.count());
        assertEquals(0, eventRepository.count());
    }

    @Test
    @Transactional
    void testDeposit_InvalidAmount() throws Exception {
        // Arrange
        UUID walletId = UUID.randomUUID();
        createWallet(walletId, new BigDecimal("200.00"));

        // Act
        mockMvc.perform(post("/api/command/wallets/{walletId}/deposit", walletId)
                        .param("amount", "-100.00")
                        .param("description", "Test deposit")
                        .contentType(MediaType.APPLICATION_FORM_URLENCODED))
                .andExpect(status().isBadRequest());

        // Assert
        WalletBalance balance = walletBalanceRepository.findByWalletIdWithLock(walletId).orElseThrow();
        assertEquals(new BigDecimal("200.00"), balance.getBalance());
        assertEquals(0, transactionRepository.count());
        assertEquals(0, eventRepository.count());
    }

    @Test
    @Transactional
    void testWithdraw_Success() throws Exception {
        // Arrange
        UUID walletId = UUID.randomUUID();
        createWallet(walletId, new BigDecimal("200.00"));

        // Act
        mockMvc.perform(post("/api/command/wallets/{walletId}/withdraw", walletId)
                        .param("amount", "100.00")
                        .param("description", "Test withdraw")
                        .contentType(MediaType.APPLICATION_FORM_URLENCODED))
                .andExpect(status().isOk());

        // Assert
        WalletBalance balance = walletBalanceRepository.findByWalletIdWithLock(walletId).orElseThrow();
        assertEquals(new BigDecimal("100.00"), balance.getBalance());
        assertEquals(1, transactionRepository.count());
        assertEquals(1, eventRepository.count());
    }

    @Test
    @Transactional
    void testWithdraw_InsufficientBalance() throws Exception {
        // Arrange
        UUID walletId = UUID.randomUUID();
        createWallet(walletId, new BigDecimal("50.00"));

        // Act
        mockMvc.perform(post("/api/command/wallets/{walletId}/withdraw", walletId)
                        .param("amount", "100.00")
                        .param("description", "Test withdraw")
                        .contentType(MediaType.APPLICATION_FORM_URLENCODED))
                .andExpect(status().isBadRequest());

        // Assert
        WalletBalance balance = walletBalanceRepository.findByWalletIdWithLock(walletId).orElseThrow();
        assertEquals(new BigDecimal("50.00"), balance.getBalance());
        assertEquals(0, transactionRepository.count());
        assertEquals(0, eventRepository.count());
    }

    @Test
    @Transactional
    void testWithdraw_WalletNotFound() throws Exception {
        // Arrange
        UUID walletId = UUID.randomUUID();

        // Act
        mockMvc.perform(post("/api/command/wallets/{walletId}/withdraw", walletId)
                        .param("amount", "100.00")
                        .param("description", "Test withdraw")
                        .contentType(MediaType.APPLICATION_FORM_URLENCODED))
                .andExpect(status().isNotFound());

        // Assert
        assertEquals(0, walletBalanceRepository.count());
        assertEquals(0, transactionRepository.count());
        assertEquals(0, eventRepository.count());
    }

    @Test
    @Transactional
    void testTransfer_Success() throws Exception {
        // Arrange
        UUID sourceWalletId = UUID.randomUUID();
        UUID targetWalletId = UUID.randomUUID();
        createWallet(sourceWalletId, new BigDecimal("200.00"));
        createWallet(targetWalletId, new BigDecimal("50.00"));

        // Act
        mockMvc.perform(post("/api/command/wallets/transfer")
                        .param("fromWalletId", sourceWalletId.toString())
                        .param("toWalletId", targetWalletId.toString())
                        .param("amount", "100.00")
                        .param("description", "Test transfer")
                        .contentType(MediaType.APPLICATION_FORM_URLENCODED))
                .andExpect(status().isOk());

        // Assert
        WalletBalance sourceBalance = walletBalanceRepository.findByWalletIdWithLock(sourceWalletId).orElseThrow();
        WalletBalance targetBalance = walletBalanceRepository.findByWalletIdWithLock(targetWalletId).orElseThrow();
        assertEquals(new BigDecimal("100.00"), sourceBalance.getBalance());
        assertEquals(new BigDecimal("150.00"), targetBalance.getBalance());
        assertEquals(2, transactionRepository.count());
        assertEquals(1, eventRepository.count());
    }

    @Test
    @Transactional
    void testTransfer_InsufficientBalance() throws Exception {
        // Arrange
        UUID sourceWalletId = UUID.randomUUID();
        UUID targetWalletId = UUID.randomUUID();
        createWallet(sourceWalletId, new BigDecimal("50.00"));
        createWallet(targetWalletId, new BigDecimal("50.00"));

        // Act
        mockMvc.perform(post("/api/command/wallets/transfer")
                        .param("fromWalletId", sourceWalletId.toString())
                        .param("toWalletId", targetWalletId.toString())
                        .param("amount", "100.00")
                        .param("description", "Test transfer")
                        .contentType(MediaType.APPLICATION_FORM_URLENCODED))
                .andExpect(status().isBadRequest());

        // Assert
        WalletBalance sourceBalance = walletBalanceRepository.findByWalletIdWithLock(sourceWalletId).orElseThrow();
        WalletBalance targetBalance = walletBalanceRepository.findByWalletIdWithLock(targetWalletId).orElseThrow();
        assertEquals(new BigDecimal("50.00"), sourceBalance.getBalance());
        assertEquals(new BigDecimal("50.00"), targetBalance.getBalance());
        assertEquals(0, transactionRepository.count());
        assertEquals(0, eventRepository.count());
    }

    @Test
    @Transactional
    void testTransfer_SourceWalletNotFound() throws Exception {
        // Arrange
        UUID sourceWalletId = UUID.randomUUID();
        UUID targetWalletId = UUID.randomUUID();
        createWallet(targetWalletId, new BigDecimal("50.00"));

        // Act
        mockMvc.perform(post("/api/command/wallets/transfer")
                        .param("fromWalletId", sourceWalletId.toString())
                        .param("toWalletId", targetWalletId.toString())
                        .param("amount", "100.00")
                        .param("description", "Test transfer")
                        .contentType(MediaType.APPLICATION_FORM_URLENCODED))
                .andExpect(status().isNotFound());

        // Assert
        WalletBalance targetBalance = walletBalanceRepository.findByWalletIdWithLock(targetWalletId).orElseThrow();
        assertEquals(new BigDecimal("50.00"), targetBalance.getBalance());
        assertEquals(0, transactionRepository.count());
        assertEquals(0, eventRepository.count());
    }

    @Test
    @Transactional
    void testTransfer_TargetWalletNotFound() throws Exception {
        // Arrange
        UUID sourceWalletId = UUID.randomUUID();
        UUID targetWalletId = UUID.randomUUID();
        createWallet(sourceWalletId, new BigDecimal("200.00"));

        // Act
        mockMvc.perform(post("/api/command/wallets/transfer")
                        .param("fromWalletId", sourceWalletId.toString())
                        .param("toWalletId", targetWalletId.toString())
                        .param("amount", "100.00")
                        .param("description", "Test transfer")
                        .contentType(MediaType.APPLICATION_FORM_URLENCODED))
                .andExpect(status().isNotFound());

        // Assert
        WalletBalance sourceBalance = walletBalanceRepository.findByWalletIdWithLock(sourceWalletId).orElseThrow();
        assertEquals(new BigDecimal("200.00"), sourceBalance.getBalance());
        assertEquals(0, transactionRepository.count());
        assertEquals(0, eventRepository.count());
    }
}
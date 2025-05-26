package com.recargapay.wallet.query.service;

import com.recargapay.wallet.query.document.DailyBalanceDocument;
import com.recargapay.wallet.query.document.TransactionHistoryDocument;
import com.recargapay.wallet.query.document.WalletBalanceDocument;
import com.recargapay.wallet.query.repository.DailyBalanceMongoRepository;
import com.recargapay.wallet.query.repository.TransactionHistoryMongoRepository;
import com.recargapay.wallet.query.repository.WalletBalanceMongoRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;

/**
 * Service for querying wallet information with caching support.
 */
@Service
public class WalletQueryService {
    private static final Logger LOGGER = LoggerFactory.getLogger(WalletQueryService.class);

    private final WalletBalanceMongoRepository walletBalanceMongoRepository;
    private final TransactionHistoryMongoRepository transactionHistoryMongoRepository;
    private final DailyBalanceMongoRepository dailyBalanceMongoRepository;
    private final CacheService cacheService;

    public WalletQueryService(
            WalletBalanceMongoRepository walletBalanceMongoRepository,
            TransactionHistoryMongoRepository transactionHistoryMongoRepository,
            DailyBalanceMongoRepository dailyBalanceMongoRepository,
            CacheService cacheService) {
        this.walletBalanceMongoRepository = walletBalanceMongoRepository;
        this.transactionHistoryMongoRepository = transactionHistoryMongoRepository;
        this.dailyBalanceMongoRepository = dailyBalanceMongoRepository;
        this.cacheService = cacheService;
    }

    /**
     * Retrieves the current balance for a wallet, checking cache first.
     *
     * @param walletId The UUID of the wallet
     * @return The WalletBalanceDocument
     * @throws NoSuchElementException if the wallet balance is not found
     */
    public WalletBalanceDocument getBalance(UUID walletId) {
        // Check cache
        WalletBalanceDocument cachedBalance = cacheService.getBalanceFromCache(walletId);
        if (cachedBalance != null) {
            LOGGER.info("Returning cached balance for walletId={}", walletId);
            return cachedBalance;
        }

        // Cache miss, query database
        WalletBalanceDocument balance = walletBalanceMongoRepository.findById(walletId)
                .orElseThrow(() -> new NoSuchElementException("Wallet balance not found for walletId: " + walletId));

        // Cache the result
        cacheService.cacheBalance(walletId, balance);
        LOGGER.info("Queried and cached balance for walletId={}", walletId);
        return balance;
    }

    /**
     * Retrieves the transaction history for a wallet, checking cache first.
     *
     * @param walletId The UUID of the wallet
     * @return A list of TransactionHistoryDocument
     */
    public List<TransactionHistoryDocument> getTransactionHistory(UUID walletId) {
        // Check cache
        List<TransactionHistoryDocument> cachedHistory = cacheService.getTransactionHistoryFromCache(walletId);
        if (cachedHistory != null) {
            LOGGER.info("Returning cached transaction history for walletId={}", walletId);
            return cachedHistory;
        }

        // Cache miss, query database
        List<TransactionHistoryDocument> history = transactionHistoryMongoRepository.findByWalletIdOrderByCreatedAtDesc(walletId);

        // Cache the result
        cacheService.cacheTransactionHistory(walletId, history);
        LOGGER.info("Queried and cached transaction history for walletId={}", walletId);
        return history;
    }

    /**
     * Retrieves the historical balance for a wallet on a specific date, checking cache first.
     *
     * @param walletId The UUID of the wallet
     * @param date The date to query
     * @return The DailyBalanceDocument
     * @throws NoSuchElementException if the historical balance is not found
     */
    public DailyBalanceDocument getHistoricalBalance(UUID walletId, LocalDate date) {
        String dateString = date.toString();
        // Check cache
        DailyBalanceDocument cachedBalance = cacheService.getHistoricalBalanceFromCache(walletId, dateString);
        if (cachedBalance != null) {
            LOGGER.info("Returning cached historical balance for walletId={}, date={}", walletId, dateString);
            return cachedBalance;
        }

        // Cache miss, query database
        DailyBalanceDocument dailyBalance = dailyBalanceMongoRepository.findByWalletIdAndDate(walletId, dateString)
                .orElseThrow(() -> new NoSuchElementException("Historical balance not found for walletId: " + walletId + " and date: " + date));

        // Cache the result
        cacheService.cacheHistoricalBalance(walletId, dateString, dailyBalance);
        LOGGER.info("Queried and cached historical balance for walletId={}, date={}", walletId, dateString);
        return dailyBalance;
    }
}
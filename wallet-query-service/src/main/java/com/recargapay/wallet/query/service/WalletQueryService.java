package com.recargapay.wallet.query.service;

import com.recargapay.wallet.query.document.DailyBalanceDocument;
import com.recargapay.wallet.query.document.WalletBalanceDocument;
import com.recargapay.wallet.query.document.TransactionHistoryDocument;
import com.recargapay.wallet.query.repository.DailyBalanceMongoRepository;
import com.recargapay.wallet.query.repository.WalletBalanceMongoRepository;
import com.recargapay.wallet.query.repository.TransactionHistoryMongoRepository;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;

@Service
public class WalletQueryService {
    private final WalletBalanceMongoRepository walletBalanceMongoRepository;
    private final TransactionHistoryMongoRepository transactionHistoryMongoRepository;
    private final DailyBalanceMongoRepository dailyBalanceMongoRepository;

    public WalletQueryService(
            WalletBalanceMongoRepository walletBalanceMongoRepository,
            TransactionHistoryMongoRepository transactionHistoryMongoRepository,
            DailyBalanceMongoRepository dailyBalanceMongoRepository) {
        this.walletBalanceMongoRepository = walletBalanceMongoRepository;
        this.transactionHistoryMongoRepository = transactionHistoryMongoRepository;
        this.dailyBalanceMongoRepository = dailyBalanceMongoRepository;
    }

    /**
     * Retrieves the current balance for a wallet.
     *
     * @param walletId the UUID of the wallet
     * @return the WalletBalanceDocument
     * @throws NoSuchElementException if the wallet balance is not found
     */
    public WalletBalanceDocument getBalance(UUID walletId) {
        return walletBalanceMongoRepository.findById(walletId)
                .orElseThrow(() -> new NoSuchElementException("Wallet balance not found for walletId: " + walletId));
    }

    /**
     * Retrieves the transaction history for a wallet, ordered by creation date descending.
     *
     * @param walletId the UUID of the wallet
     * @return a list of TransactionHistoryDocument
     */
    public List<TransactionHistoryDocument> getTransactionHistory(UUID walletId) {
        return transactionHistoryMongoRepository.findByWalletIdOrderByCreatedAtDesc(walletId);
    }

    /**
     * Retrieves the historical balance for a wallet on a specific date.
     *
     * @param walletId the UUID of the wallet
     * @param date     the date to query
     * @return the DailyBalanceDocument
     * @throws NoSuchElementException if the historical balance is not found
     */
    public DailyBalanceDocument getHistoricalBalance(UUID walletId, LocalDate date) {
        String dateString = date.toString();
        return dailyBalanceMongoRepository.findByWalletIdAndDate(walletId, dateString)
                .orElseThrow(() -> new NoSuchElementException("Historical balance not found for walletId: " + walletId + " and date: " + date));
    }
}

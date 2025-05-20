package com.recargapay.wallet.query.repository;

import com.recargapay.wallet.query.document.TransactionHistoryDocument;
import org.springframework.data.mongodb.repository.MongoRepository;
import java.util.List;
import java.util.UUID;

public interface TransactionHistoryMongoRepository extends MongoRepository<TransactionHistoryDocument, UUID> {
    /**
     * Finds transaction history for a wallet, ordered by creation date descending.
     *
     * @param walletId the UUID of the wallet
     * @return a list of TransactionHistoryDocument
     */
    List<TransactionHistoryDocument> findByWalletIdOrderByCreatedAtDesc(UUID walletId);
}

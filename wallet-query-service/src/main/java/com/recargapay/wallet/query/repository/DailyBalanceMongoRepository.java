package com.recargapay.wallet.query.repository;

import com.recargapay.wallet.query.document.DailyBalanceDocument;
import org.springframework.data.mongodb.repository.MongoRepository;
import java.util.Optional;
import java.util.UUID;

public interface DailyBalanceMongoRepository extends MongoRepository<DailyBalanceDocument, String> {
    /**
     * Finds a DailyBalanceDocument by walletId and date.
     *
     * @param walletId the UUID of the wallet
     * @param date     the date in YYYY-MM-DD format
     * @return an Optional containing the DailyBalanceDocument if found, or empty otherwise
     */
    Optional<DailyBalanceDocument> findByWalletIdAndDate(UUID walletId, String date);
}

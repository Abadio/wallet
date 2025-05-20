package com.recargapay.wallet.consumer.repository;

import com.recargapay.wallet.consumer.document.DailyBalanceDocument;
import org.springframework.data.mongodb.repository.MongoRepository;
import java.util.Optional;
import java.util.UUID;

public interface DailyBalanceMongoRepository extends MongoRepository<DailyBalanceDocument, String> {
    Optional<DailyBalanceDocument> findByWalletIdAndDate(UUID walletId, String date);
}
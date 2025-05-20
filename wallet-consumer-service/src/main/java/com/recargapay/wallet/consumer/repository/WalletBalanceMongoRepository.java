package com.recargapay.wallet.consumer.repository;

import com.recargapay.wallet.consumer.document.WalletBalanceDocument;
import org.springframework.data.mongodb.repository.MongoRepository;
import java.util.UUID;

public interface WalletBalanceMongoRepository extends MongoRepository<WalletBalanceDocument, UUID> {
}

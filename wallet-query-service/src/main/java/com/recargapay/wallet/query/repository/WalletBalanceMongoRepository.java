package com.recargapay.wallet.query.repository;

import com.recargapay.wallet.query.document.WalletBalanceDocument;
import org.springframework.data.mongodb.repository.MongoRepository;
import java.util.UUID;

public interface WalletBalanceMongoRepository extends MongoRepository<WalletBalanceDocument, UUID> {
}

package com.recargapay.wallet.consumer.repository;

import com.recargapay.wallet.consumer.document.TransactionHistoryDocument;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface TransactionHistoryMongoRepository extends MongoRepository<TransactionHistoryDocument, UUID> {
    boolean existsById(UUID id);
}
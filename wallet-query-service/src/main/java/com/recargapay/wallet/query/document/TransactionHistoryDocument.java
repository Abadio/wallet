package com.recargapay.wallet.query.document;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

@Document(collection = "transaction_history")
public class TransactionHistoryDocument {
    @Id
    private UUID id;
    private UUID walletId;
    private UUID userId;
    private String username;
    private String transactionType;
    private BigDecimal amount;
    private BigDecimal balanceAfter;
    private UUID relatedWalletId;
    private String relatedUsername;
    private String description;
    private OffsetDateTime createdAt;

    public TransactionHistoryDocument() {
    }

    public TransactionHistoryDocument(UUID id, UUID walletId, UUID userId, String username, String transactionType, BigDecimal amount, BigDecimal balanceAfter, UUID relatedWalletId, String relatedUsername, String description, OffsetDateTime createdAt) {
        this.id = id;
        this.walletId = walletId;
        this.userId = userId;
        this.username = username;
        this.transactionType = transactionType;
        this.amount = amount;
        this.balanceAfter = balanceAfter;
        this.relatedWalletId = relatedWalletId;
        this.relatedUsername = relatedUsername;
        this.description = description;
        this.createdAt = createdAt;
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public UUID getWalletId() {
        return walletId;
    }

    public void setWalletId(UUID walletId) {
        this.walletId = walletId;
    }

    public UUID getUserId() {
        return userId;
    }

    public void setUserId(UUID userId) {
        this.userId = userId;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getTransactionType() {
        return transactionType;
    }

    public void setTransactionType(String transactionType) {
        this.transactionType = transactionType;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public BigDecimal getBalanceAfter() {
        return balanceAfter;
    }

    public void setBalanceAfter(BigDecimal balanceAfter) {
        this.balanceAfter = balanceAfter;
    }

    public UUID getRelatedWalletId() {
        return relatedWalletId;
    }

    public void setRelatedWalletId(UUID relatedWalletId) {
        this.relatedWalletId = relatedWalletId;
    }

    public String getRelatedUsername() {
        return relatedUsername;
    }

    public void setRelatedUsername(String relatedUsername) {
        this.relatedUsername = relatedUsername;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(OffsetDateTime createdAt) {
        this.createdAt = createdAt;
    }
}

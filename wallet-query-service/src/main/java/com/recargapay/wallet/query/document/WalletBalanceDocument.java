package com.recargapay.wallet.query.document;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

@Document(collection = "wallet_balances")
public class WalletBalanceDocument {
    @Id
    private UUID id;
    private UUID userId;
    private String username;
    private String currency;
    private BigDecimal balance;
    private UUID lastTransactionId;
    private OffsetDateTime updatedAt;

    public WalletBalanceDocument() {
    }

    public WalletBalanceDocument(UUID id, UUID userId, String username, String currency, BigDecimal balance, UUID lastTransactionId, OffsetDateTime updatedAt) {
        this.id = id;
        this.userId = userId;
        this.username = username;
        this.currency = currency;
        this.balance = balance;
        this.lastTransactionId = lastTransactionId;
        this.updatedAt = updatedAt;
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
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

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public BigDecimal getBalance() {
        return balance;
    }

    public void setBalance(BigDecimal balance) {
        this.balance = balance;
    }

    public UUID getLastTransactionId() {
        return lastTransactionId;
    }

    public void setLastTransactionId(UUID lastTransactionId) {
        this.lastTransactionId = lastTransactionId;
    }

    public OffsetDateTime getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(OffsetDateTime updatedAt) {
        this.updatedAt = updatedAt;
    }
}

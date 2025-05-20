package com.recargapay.wallet.query.document;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.CompoundIndexes;
import org.springframework.data.mongodb.core.mapping.Document;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

@Document(collection = "daily_balance")
@CompoundIndexes({
        @CompoundIndex(name = "walletId_date_idx", def = "{'walletId': 1, 'date': 1}")
})
public class DailyBalanceDocument {
    @Id
    private String id;
    private UUID walletId;
    private String date;
    private UUID userId;
    private String username;
    private BigDecimal balance;
    private UUID lastTransactionId;
    private OffsetDateTime updatedAt;

    public DailyBalanceDocument() {
    }

    public DailyBalanceDocument(UUID walletId, String date, UUID userId, String username, 
                               BigDecimal balance, UUID lastTransactionId, OffsetDateTime updatedAt) {
        this.id = generateId(walletId, date);
        this.walletId = walletId;
        this.date = date;
        this.userId = userId;
        this.username = username;
        this.balance = balance;
        this.lastTransactionId = lastTransactionId;
        this.updatedAt = updatedAt;
    }

    private static String generateId(UUID walletId, String date) {
        return walletId.toString() + "_" + date;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public UUID getWalletId() {
        return walletId;
    }

    public void setWalletId(UUID walletId) {
        this.walletId = walletId;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
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

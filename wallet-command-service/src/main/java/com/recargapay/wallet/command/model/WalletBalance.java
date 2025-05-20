package com.recargapay.wallet.command.model;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Table(name = "wallet_balances", schema = "wallet_service")
public class WalletBalance {
    @Id
    @Column(name = "wallet_id")
    private UUID walletId;

    @Column(name = "balance", nullable = false)
    private BigDecimal balance;

    @Column(name = "last_transaction_id", nullable = false)
    private UUID lastTransactionId;

    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt;

    public WalletBalance() {
        this.balance = BigDecimal.ZERO;
        this.lastTransactionId = new UUID(0L, 0L);
        this.updatedAt = OffsetDateTime.now();
    }

    public WalletBalance(UUID walletId) {
        this();
        if (walletId == null) {
            throw new IllegalArgumentException("Wallet ID must not be null");
        }
        this.walletId = walletId;
    }

    public UUID getWalletId() {
        return walletId;
    }

    public void setWalletId(UUID walletId) {
        if (walletId == null) {
            throw new IllegalArgumentException("Wallet ID must not be null");
        }
        this.walletId = walletId;
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
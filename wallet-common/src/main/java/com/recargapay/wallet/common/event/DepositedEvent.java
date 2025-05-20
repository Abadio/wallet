package com.recargapay.wallet.common.event;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

public class DepositedEvent {
    private UUID transactionId;
    private UUID walletId;
    private BigDecimal amount;
    private BigDecimal balanceAfter;
    private String description;
    private OffsetDateTime createdAt;

    public DepositedEvent() {
    }

    public DepositedEvent(UUID transactionId, UUID walletId, BigDecimal amount, BigDecimal balanceAfter, String description, OffsetDateTime createdAt) {
        this.transactionId = transactionId;
        this.walletId = walletId;
        this.amount = amount;
        this.balanceAfter = balanceAfter;
        this.description = description;
        this.createdAt = createdAt;
    }

    public UUID getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(UUID transactionId) {
        this.transactionId = transactionId;
    }

    public UUID getWalletId() {
        return walletId;
    }

    public void setWalletId(UUID walletId) {
        this.walletId = walletId;
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

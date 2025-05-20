package com.recargapay.wallet.common.event;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

public class TransferredEvent {
    private UUID transactionId;
    private UUID fromWalletId;
    private UUID toWalletId;
    private BigDecimal amount;
    private BigDecimal fromBalanceAfter;
    private BigDecimal toBalanceAfter;
    private String description;
    private OffsetDateTime createdAt;
    private String transactionType; // Novo campo para indicar TRANSFER_SENT ou TRANSFER_RECEIVED

    // Construtor padrão para Jackson
    public TransferredEvent() {
        this.transactionId = null;
        this.fromWalletId = null;
        this.toWalletId = null;
        this.amount = BigDecimal.ZERO;
        this.fromBalanceAfter = BigDecimal.ZERO;
        this.toBalanceAfter = BigDecimal.ZERO;
        this.description = "";
        this.createdAt = null;
        this.transactionType = "";
    }

    public TransferredEvent(
            UUID transactionId,
            UUID fromWalletId,
            UUID toWalletId,
            BigDecimal amount,
            BigDecimal fromBalanceAfter,
            BigDecimal toBalanceAfter,
            String description,
            OffsetDateTime createdAt,
            String transactionType) {
        this.transactionId = transactionId;
        this.fromWalletId = fromWalletId;
        this.toWalletId = toWalletId;
        this.amount = amount;
        this.fromBalanceAfter = fromBalanceAfter;
        this.toBalanceAfter = toBalanceAfter;
        this.description = description;
        this.createdAt = createdAt;
        this.transactionType = transactionType;
    }

    public UUID getTransactionId() {
        return transactionId;
    }

    public UUID getFromWalletId() {
        return fromWalletId;
    }

    public UUID getToWalletId() {
        return toWalletId;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public BigDecimal getFromBalanceAfter() {
        return fromBalanceAfter;
    }

    public BigDecimal getToBalanceAfter() {
        return toBalanceAfter;
    }

    public String getDescription() {
        return description;
    }

    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }

    public String getTransactionType() {
        return transactionType;
    }

    @Override
    public String toString() {
        return "TransferredEvent{" +
                "transactionId=" + transactionId +
                ", fromWalletId=" + fromWalletId +
                ", toWalletId=" + toWalletId +
                ", amount=" + amount +
                ", fromBalanceAfter=" + fromBalanceAfter +
                ", toBalanceAfter=" + toBalanceAfter +
                ", description='" + description + '\'' +
                ", createdAt=" + createdAt +
                ", transactionType='" + transactionType + '\'' +
                '}';
    }
}
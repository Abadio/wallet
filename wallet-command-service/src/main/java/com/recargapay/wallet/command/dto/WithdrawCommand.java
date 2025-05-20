package com.recargapay.wallet.command.dto;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

public class WithdrawCommand {
    private UUID walletId;
    private BigDecimal amount;
    private String description;
    private OffsetDateTime createdAt;

    public WithdrawCommand() {
    }

    public WithdrawCommand(UUID walletId, BigDecimal amount, String description, OffsetDateTime createdAt) {
        this.walletId = walletId;
        this.amount = amount;
        this.description = description;
        this.createdAt = createdAt;
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

package com.recargapay.wallet.command.repository;

import com.recargapay.wallet.command.model.WalletBalance;
import org.springframework.context.annotation.Profile;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;
import java.util.UUID;

@Profile("integration")
public interface WalletBalanceTestRepository extends JpaRepository<WalletBalance, UUID> {

    @Query("SELECT wb FROM WalletBalance wb WHERE wb.walletId = :walletId")
    Optional<WalletBalance> findByWalletIdWithLock(@Param("walletId") UUID walletId);
}
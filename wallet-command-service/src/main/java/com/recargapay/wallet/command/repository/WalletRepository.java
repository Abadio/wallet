package com.recargapay.wallet.command.repository;

import com.recargapay.wallet.command.model.Wallet;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.UUID;

public interface WalletRepository extends JpaRepository<Wallet, UUID> {
}

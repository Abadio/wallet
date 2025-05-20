package com.recargapay.wallet.consumer.repository;

import com.recargapay.wallet.consumer.model.Wallet;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.UUID;

public interface WalletRepository extends JpaRepository<Wallet, UUID> {
}

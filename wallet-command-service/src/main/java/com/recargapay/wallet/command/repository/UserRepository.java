package com.recargapay.wallet.command.repository;

import com.recargapay.wallet.command.model.User;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.UUID;

public interface UserRepository extends JpaRepository<User, UUID> {
}

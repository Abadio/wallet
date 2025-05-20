package com.recargapay.wallet.command.repository;

import com.recargapay.wallet.command.model.Event;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.UUID;

public interface EventRepository extends JpaRepository<Event, UUID> {
}

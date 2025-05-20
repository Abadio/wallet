package com.recargapay.wallet.command.controller;

import com.recargapay.wallet.command.dto.DepositCommand;
import com.recargapay.wallet.command.dto.WithdrawCommand;
import com.recargapay.wallet.command.dto.TransferCommand;
import com.recargapay.wallet.command.service.WalletCommandService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.NoSuchElementException;
import java.util.UUID;

@RestController
@RequestMapping("/api/command/wallets")
@Tag(name = "Wallet Command API", description = "API for performing write operations on wallets")
public class WalletCommandController {
    private final WalletCommandService commandService;

    public WalletCommandController(WalletCommandService commandService) {
        this.commandService = commandService;
    }

    @PostMapping("/{walletId}/deposit")
    @Operation(
            summary = "Deposit funds into a wallet",
            description = "Deposits a specified amount into a wallet, creating a DEPOSIT transaction and updating the balance.",
            responses = {
                    @ApiResponse(responseCode = "200", description = "Deposit successful"),
                    @ApiResponse(responseCode = "400", description = "Invalid amount or parameters"),
                    @ApiResponse(responseCode = "404", description = "Wallet not found")
            }
    )
    public ResponseEntity<Void> deposit(
            @Parameter(description = "UUID of the wallet", required = true) @PathVariable UUID walletId,
            @Parameter(description = "Amount to deposit (must be positive)", required = true) @RequestParam BigDecimal amount,
            @Parameter(description = "Description of the transaction") @RequestParam(required = false) String description) {
        commandService.handle(new DepositCommand(walletId, amount, description, OffsetDateTime.now()));
        return ResponseEntity.ok().build();
    }

    @PostMapping("/{walletId}/withdraw")
    @Operation(
            summary = "Withdraw funds from a wallet",
            description = "Withdraws a specified amount from a wallet, creating a WITHDRAWAL transaction and updating the balance.",
            responses = {
                    @ApiResponse(responseCode = "200", description = "Withdrawal successful"),
                    @ApiResponse(responseCode = "400", description = "Invalid amount or insufficient balance"),
                    @ApiResponse(responseCode = "404", description = "Wallet not found")
            }
    )
    public ResponseEntity<Void> withdraw(
            @Parameter(description = "UUID of the wallet", required = true) @PathVariable UUID walletId,
            @Parameter(description = "Amount to withdraw (must be positive)", required = true) @RequestParam BigDecimal amount,
            @Parameter(description = "Description of the transaction") @RequestParam(required = false) String description) {
        commandService.handle(new WithdrawCommand(walletId, amount, description, OffsetDateTime.now()));
        return ResponseEntity.ok().build();
    }

    @PostMapping("/transfer")
    @Operation(
            summary = "Transfer funds between wallets",
            description = "Transfers a specified amount from one wallet to another, creating TRANSFER_SENT and TRANSFER_RECEIVED transactions.",
            responses = {
                    @ApiResponse(responseCode = "200", description = "Transfer successful"),
                    @ApiResponse(responseCode = "400", description = "Invalid amount, same wallet, or insufficient balance"),
                    @ApiResponse(responseCode = "404", description = "One or both wallets not found")
            }
    )
    public ResponseEntity<Void> transfer(
            @Parameter(description = "UUID of the source wallet", required = true) @RequestParam UUID fromWalletId,
            @Parameter(description = "UUID of the destination wallet", required = true) @RequestParam UUID toWalletId,
            @Parameter(description = "Amount to transfer (must be positive)", required = true) @RequestParam BigDecimal amount,
            @Parameter(description = "Description of the transaction") @RequestParam(required = false) String description) {
        commandService.handle(new TransferCommand(fromWalletId, toWalletId, amount, description, OffsetDateTime.now()));
        return ResponseEntity.ok().build();
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<String> handleIllegalArgumentException(IllegalArgumentException ex) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(ex.getMessage());
    }

    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<String> handleIllegalStateException(IllegalStateException ex) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(ex.getMessage());
    }

    @ExceptionHandler(NoSuchElementException.class)
    public ResponseEntity<String> handleNoSuchElementException(NoSuchElementException ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(ex.getMessage());
    }
}
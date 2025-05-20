package com.recargapay.wallet.query.controller;

import com.recargapay.wallet.query.document.DailyBalanceDocument;
import com.recargapay.wallet.query.document.WalletBalanceDocument;
import com.recargapay.wallet.query.document.TransactionHistoryDocument;
import com.recargapay.wallet.query.service.WalletQueryService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/query/wallets")
@Tag(name = "Wallet Query API", description = "API for querying wallet information")
public class WalletQueryController {
    private final WalletQueryService queryService;

    public WalletQueryController(WalletQueryService queryService) {
        this.queryService = queryService;
    }

    @GetMapping("/{walletId}/balance")
    @Operation(
            summary = "Get current wallet balance",
            description = "Retrieves the current balance and related information for a specified wallet.",
            responses = {
                    @ApiResponse(responseCode = "200", description = "Balance retrieved successfully"),
                    @ApiResponse(responseCode = "404", description = "Wallet not found")
            }
    )
    public ResponseEntity<WalletBalanceDocument> getBalance(
            @Parameter(description = "UUID of the wallet", required = true) @PathVariable UUID walletId) {
        return ResponseEntity.ok(queryService.getBalance(walletId));
    }

    @GetMapping("/{walletId}/history")
    @Operation(
            summary = "Get transaction history",
            description = "Retrieves the transaction history for a specified wallet (last 30 days).",
            responses = {
                    @ApiResponse(responseCode = "200", description = "Transaction history retrieved successfully"),
                    @ApiResponse(responseCode = "404", description = "Wallet not found")
            }
    )
    public ResponseEntity<List<TransactionHistoryDocument>> getTransactionHistory(
            @Parameter(description = "UUID of the wallet", required = true) @PathVariable UUID walletId) {
        return ResponseEntity.ok(queryService.getTransactionHistory(walletId));
    }

    @GetMapping("/{walletId}/historical-balance")
    @Operation(
            summary = "Get historical wallet balance",
            description = "Retrieves the balance of a wallet on a specific date.",
            responses = {
                    @ApiResponse(responseCode = "200", description = "Historical balance retrieved successfully"),
                    @ApiResponse(responseCode = "400", description = "Invalid date format"),
                    @ApiResponse(responseCode = "404", description = "Balance not found for the specified date")
            }
    )
    public ResponseEntity<DailyBalanceDocument> getHistoricalBalance(
            @Parameter(description = "UUID of the wallet", required = true) @PathVariable UUID walletId,
            @Parameter(description = "Date in YYYY-MM-DD format", required = true) @RequestParam String date) {
        try {
            LocalDate parsedDate = LocalDate.parse(date);
            return ResponseEntity.ok(queryService.getHistoricalBalance(walletId, parsedDate));
        } catch (DateTimeParseException ex) {
            throw new DateTimeParseException("Invalid date format. Use YYYY-MM-DD.", date, ex.getErrorIndex());
        }
    }
}
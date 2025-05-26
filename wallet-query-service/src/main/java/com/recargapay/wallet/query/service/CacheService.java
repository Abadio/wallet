package com.recargapay.wallet.query.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.recargapay.wallet.query.document.DailyBalanceDocument;
import com.recargapay.wallet.query.document.TransactionHistoryDocument;
import com.recargapay.wallet.query.document.WalletBalanceDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Service for managing cache operations in Redis.
 */
@Service
public class CacheService {
    private static final Logger LOGGER = LoggerFactory.getLogger(CacheService.class);

    private static final String BALANCE_KEY_PREFIX = "wallet:balance:";
    private static final String HISTORY_KEY_PREFIX = "wallet:history:";
    private static final String HISTORICAL_BALANCE_KEY_PREFIX = "wallet:historical_balance:";

    private final RedisTemplate<String, Object> redisTemplate;
    private final ObjectMapper objectMapper;
    private final Duration ttl;

    public CacheService(RedisTemplate<String, Object> redisTemplate,
                        ObjectMapper objectMapper,
                        @Value("${cache.ttl:30m}") Duration ttl) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
        this.ttl = ttl;
        LOGGER.info("Initialized CacheService with TTL: {}", ttl);
    }

    /**
     * Retrieves the wallet balance from cache.
     *
     * @param walletId UUID of the wallet
     * @return WalletBalanceDocument if found, null otherwise
     */
    public WalletBalanceDocument getBalanceFromCache(UUID walletId) {
        String key = BALANCE_KEY_PREFIX + walletId;
        try {
            Object cached = redisTemplate.opsForValue().get(key);
            LOGGER.debug("Retrieved from cache: key={}, value={}", key, cached);
            if (cached == null) {
                LOGGER.debug("Cache miss for balance: walletId={}, key={}", walletId, key);
                return null;
            }
            if (cached instanceof WalletBalanceDocument) {
                LOGGER.debug("Cache hit for balance: walletId={}, key={}", walletId, key);
                return (WalletBalanceDocument) cached;
            }
            // Fallback: try to convert from LinkedHashMap
            LOGGER.warn("Cached object is not WalletBalanceDocument, attempting conversion: type={}, key={}", cached.getClass().getName(), key);
            return objectMapper.convertValue(cached, WalletBalanceDocument.class);
        } catch (Exception e) {
            LOGGER.error("Error retrieving balance from cache: walletId={}, key={}", walletId, key, e);
            return null;
        }
    }

    /**
     * Stores the wallet balance in cache with TTL.
     *
     * @param walletId UUID of the wallet
     * @param balance WalletBalanceDocument to cache
     */
    public void cacheBalance(UUID walletId, WalletBalanceDocument balance) {
        String key = BALANCE_KEY_PREFIX + walletId;
        try {
            if (balance == null) {
                LOGGER.warn("Attempted to cache null balance: walletId={}, key={}", walletId, key);
                return;
            }
            redisTemplate.opsForValue().set(key, balance, ttl);
            LOGGER.debug("Cached balance: walletId={}, key={}, value={}", walletId, key, balance);
        } catch (Exception e) {
            LOGGER.error("Error caching balance: walletId={}, key={}", walletId, key, e);
            throw new RuntimeException("Failed to cache balance for walletId: " + walletId, e);
        }
    }

    /**
     * Retrieves the transaction history from cache.
     *
     * @param walletId UUID of the wallet
     * @return List of TransactionHistoryDocument if found, null otherwise
     */
    public List<TransactionHistoryDocument> getTransactionHistoryFromCache(UUID walletId) {
        String key = HISTORY_KEY_PREFIX + walletId;
        try {
            Object cached = redisTemplate.opsForValue().get(key);
            LOGGER.debug("Retrieved from cache: key={}, value={}", key, cached);
            if (cached == null) {
                LOGGER.debug("Cache miss for transaction history: walletId={}, key={}", walletId, key);
                return null;
            }
            if (cached instanceof List) {
                List<?> list = (List<?>) cached;
                if (!list.isEmpty() && list.get(0) instanceof TransactionHistoryDocument) {
                    LOGGER.debug("Cache hit for transaction history: walletId={}, key={}", walletId, key);
                    return list.stream()
                            .map(item -> (TransactionHistoryDocument) item)
                            .collect(Collectors.toList());
                }
                // Fallback: convert LinkedHashMap to TransactionHistoryDocument
                LOGGER.warn("Cached list contains non-TransactionHistoryDocument, attempting conversion: type={}, key={}", list.isEmpty() ? "empty" : list.get(0).getClass().getName(), key);
                return list.stream()
                        .map(item -> objectMapper.convertValue(item, TransactionHistoryDocument.class))
                        .collect(Collectors.toList());
            }
            LOGGER.warn("Cache miss for transaction history: invalid type: {}, key={}", cached.getClass().getName(), key);
            return null;
        } catch (Exception e) {
            LOGGER.error("Error retrieving transaction history from cache: walletId={}, key={}", walletId, key, e);
            return null;
        }
    }

    /**
     * Stores the transaction history in cache with TTL.
     *
     * @param walletId UUID of the wallet
     * @param history List of TransactionHistoryDocument to cache
     */
    public void cacheTransactionHistory(UUID walletId, List<TransactionHistoryDocument> history) {
        String key = HISTORY_KEY_PREFIX + walletId;
        try {
            if (history == null) {
                LOGGER.warn("Attempted to cache null transaction history: walletId={}, key={}", walletId, key);
                return;
            }
            redisTemplate.opsForValue().set(key, history, ttl);
            LOGGER.debug("Cached transaction history: walletId={}, key={}, value={}", walletId, key, history);
        } catch (Exception e) {
            LOGGER.error("Error caching transaction history: walletId={}, key={}", walletId, key, e);
            throw new RuntimeException("Failed to cache transaction history for walletId: " + walletId, e);
        }
    }

    /**
     * Retrieves the historical balance from cache.
     *
     * @param walletId UUID of the wallet
     * @param date Date in YYYY-MM-DD format
     * @return DailyBalanceDocument if found, null otherwise
     */
    public DailyBalanceDocument getHistoricalBalanceFromCache(UUID walletId, String date) {
        String key = HISTORICAL_BALANCE_KEY_PREFIX + walletId + ":" + date;
        try {
            Object cached = redisTemplate.opsForValue().get(key);
            LOGGER.debug("Retrieved from cache: key={}, value={}", key, cached);
            if (cached == null) {
                LOGGER.debug("Cache miss for historical balance: walletId={}, date={}, key={}", walletId, date, key);
                return null;
            }
            if (cached instanceof DailyBalanceDocument) {
                LOGGER.debug("Cache hit for historical balance: walletId={}, date={}, key={}", walletId, date, key);
                return (DailyBalanceDocument) cached;
            }
            // Fallback: convert LinkedHashMap to DailyBalanceDocument
            LOGGER.warn("Cached object is not DailyBalanceDocument, attempting conversion: type={}, key={}", cached.getClass().getName(), key);
            return objectMapper.convertValue(cached, DailyBalanceDocument.class);
        } catch (Exception e) {
            LOGGER.error("Error retrieving historical balance from cache: walletId={}, date={}, key={}", walletId, date, key, e);
            return null;
        }
    }

    /**
     * Stores the historical balance in cache with TTL.
     *
     * @param walletId UUID of the wallet
     * @param date Date in YYYY-MM-DD format
     * @param balance DailyBalanceDocument to cache
     */
    public void cacheHistoricalBalance(UUID walletId, String date, DailyBalanceDocument balance) {
        String key = HISTORICAL_BALANCE_KEY_PREFIX + walletId + ":" + date;
        try {
            if (balance == null) {
                LOGGER.warn("Attempted to cache null historical balance: walletId={}, date={}, key={}", walletId, date, key);
                return;
            }
            redisTemplate.opsForValue().set(key, balance, ttl);
            LOGGER.debug("Cached historical balance: walletId={}, date={}, key={}, value={}", walletId, date, key, balance);
        } catch (Exception e) {
            LOGGER.error("Error caching historical balance: walletId={}, date={}, key={}", walletId, date, key, e);
            throw new RuntimeException("Failed to cache historical balance for walletId: " + walletId, e);
        }
    }

    /**
     * Invalidates cache entries for a wallet.
     *
     * @param walletId UUID of the wallet
     */
    public void invalidateCache(UUID walletId) {
        try {
            // Invalidate balance
            String balanceKey = BALANCE_KEY_PREFIX + walletId;
            redisTemplate.delete(balanceKey);
            LOGGER.debug("Invalidated balance cache: walletId={}, key={}", walletId, balanceKey);

            // Invalidate transaction history
            String historyKey = HISTORY_KEY_PREFIX + walletId;
            redisTemplate.delete(historyKey);
            LOGGER.debug("Invalidated transaction history cache: walletId={}, key={}", walletId, historyKey);

            // Invalidate historical balances using SCAN
            String pattern = HISTORICAL_BALANCE_KEY_PREFIX + walletId + ":*";
            ScanOptions scanOptions = ScanOptions.scanOptions().match(pattern).count(100).build();
            try (var cursor = redisTemplate.scan(scanOptions)) {
                while (cursor.hasNext()) {
                    String key = cursor.next();
                    redisTemplate.delete(key);
                    LOGGER.debug("Deleted historical balance key: {}", key);
                }
            }
            LOGGER.debug("Invalidated historical balance cache: walletId={}, pattern={}", walletId, pattern);
        } catch (Exception e) {
            LOGGER.error("Error invalidating cache: walletId={}", walletId, e);
            throw new RuntimeException("Failed to invalidate cache for walletId: " + walletId, e);
        }
    }
}
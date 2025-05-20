-- Schema for wallet service
CREATE SCHEMA IF NOT EXISTS wallet_service;

-- Table for users
CREATE TABLE wallet_service.users (
    user_id UUID NOT NULL,
    username VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT users_username_unique UNIQUE (username),
    CONSTRAINT users_email_unique UNIQUE (email),
    PRIMARY KEY (user_id)
);

-- Table for wallet metadata
CREATE TABLE wallet_service.wallets (
    wallet_id UUID NOT NULL,
    user_id UUID NOT NULL,
    currency VARCHAR(3) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    FOREIGN KEY (user_id) REFERENCES wallet_service.users(user_id),
    PRIMARY KEY (wallet_id)
);

CREATE INDEX idx_wallets_user_id ON wallet_service.wallets(user_id);

-- Table for current wallet balances
CREATE TABLE wallet_service.wallet_balances (
    wallet_id UUID NOT NULL,
    balance DECIMAL(19,2) NOT NULL DEFAULT 0.00,
    last_transaction_id UUID NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    FOREIGN KEY (wallet_id) REFERENCES wallet_service.wallets(wallet_id),
    CONSTRAINT positive_balance CHECK (balance >= 0),
    PRIMARY KEY (wallet_id)
);

-- Table for transaction history (partitioned by month)
CREATE TABLE wallet_service.transactions (
    transaction_id UUID NOT NULL,
    wallet_id UUID NOT NULL,
    transaction_type VARCHAR(20) NOT NULL,
    amount DECIMAL(19,2) NOT NULL,
    balance_after DECIMAL(19,2) NOT NULL,
    related_wallet_id UUID,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    FOREIGN KEY (wallet_id) REFERENCES wallet_service.wallets(wallet_id),
    FOREIGN KEY (related_wallet_id) REFERENCES wallet_service.wallets(wallet_id),
    CONSTRAINT valid_type CHECK (transaction_type IN ('DEPOSIT', 'WITHDRAWAL', 'TRANSFER_SENT', 'TRANSFER_RECEIVED'))
) PARTITION BY RANGE (created_at);

-- Monthly partitions for transactions (2025)
CREATE TABLE wallet_service.transactions_2025_01 PARTITION OF wallet_service.transactions
    FOR VALUES FROM ('2025-01-01 00:00:00+00') TO ('2025-02-01 00:00:00+00');
CREATE TABLE wallet_service.transactions_2025_02 PARTITION OF wallet_service.transactions
    FOR VALUES FROM ('2025-02-01 00:00:00+00') TO ('2025-03-01 00:00:00+00');
CREATE TABLE wallet_service.transactions_2025_03 PARTITION OF wallet_service.transactions
    FOR VALUES FROM ('2025-03-01 00:00:00+00') TO ('2025-04-01 00:00:00+00');
CREATE TABLE wallet_service.transactions_2025_04 PARTITION OF wallet_service.transactions
    FOR VALUES FROM ('2025-04-01 00:00:00+00') TO ('2025-05-01 00:00:00+00');
CREATE TABLE wallet_service.transactions_2025_05 PARTITION OF wallet_service.transactions
    FOR VALUES FROM ('2025-05-01 00:00:00+00') TO ('2025-06-01 00:00:00+00');
CREATE TABLE wallet_service.transactions_2025_06 PARTITION OF wallet_service.transactions
    FOR VALUES FROM ('2025-06-01 00:00:00+00') TO ('2025-07-01 00:00:00+00');
CREATE TABLE wallet_service.transactions_2025_07 PARTITION OF wallet_service.transactions
    FOR VALUES FROM ('2025-07-01 00:00:00+00') TO ('2025-08-01 00:00:00+00');
CREATE TABLE wallet_service.transactions_2025_08 PARTITION OF wallet_service.transactions
    FOR VALUES FROM ('2025-08-01 00:00:00+00') TO ('2025-09-01 00:00:00+00');
CREATE TABLE wallet_service.transactions_2025_09 PARTITION OF wallet_service.transactions
    FOR VALUES FROM ('2025-09-01 00:00:00+00') TO ('2025-10-01 00:00:00+00');
CREATE TABLE wallet_service.transactions_2025_10 PARTITION OF wallet_service.transactions
    FOR VALUES FROM ('2025-10-01 00:00:00+00') TO ('2025-11-01 00:00:00+00');
CREATE TABLE wallet_service.transactions_2025_11 PARTITION OF wallet_service.transactions
    FOR VALUES FROM ('2025-11-01 00:00:00+00') TO ('2025-12-01 00:00:00+00');
CREATE TABLE wallet_service.transactions_2025_12 PARTITION OF wallet_service.transactions
    FOR VALUES FROM ('2025-12-01 00:00:00+00') TO ('2026-01-01 00:00:00+00');

CREATE UNIQUE INDEX idx_transactions_2025_01_transaction_id ON wallet_service.transactions_2025_01(transaction_id);
CREATE INDEX idx_transactions_2025_01_wallet_id ON wallet_service.transactions_2025_01(wallet_id);
CREATE INDEX idx_transactions_2025_01_created_at ON wallet_service.transactions_2025_01(created_at);

CREATE UNIQUE INDEX idx_transactions_2025_02_transaction_id ON wallet_service.transactions_2025_02(transaction_id);
CREATE INDEX idx_transactions_2025_02_wallet_id ON wallet_service.transactions_2025_02(wallet_id);
CREATE INDEX idx_transactions_2025_02_created_at ON wallet_service.transactions_2025_02(created_at);

CREATE UNIQUE INDEX idx_transactions_2025_03_transaction_id ON wallet_service.transactions_2025_03(transaction_id);
CREATE INDEX idx_transactions_2025_03_wallet_id ON wallet_service.transactions_2025_03(wallet_id);
CREATE INDEX idx_transactions_2025_03_created_at ON wallet_service.transactions_2025_03(created_at);

CREATE UNIQUE INDEX idx_transactions_2025_04_transaction_id ON wallet_service.transactions_2025_04(transaction_id);
CREATE INDEX idx_transactions_2025_04_wallet_id ON wallet_service.transactions_2025_04(wallet_id);
CREATE INDEX idx_transactions_2025_04_created_at ON wallet_service.transactions_2025_04(created_at);

CREATE UNIQUE INDEX idx_transactions_2025_05_transaction_id ON wallet_service.transactions_2025_05(transaction_id);
CREATE INDEX idx_transactions_2025_05_wallet_id ON wallet_service.transactions_2025_05(wallet_id);
CREATE INDEX idx_transactions_2025_05_created_at ON wallet_service.transactions_2025_05(created_at);

CREATE UNIQUE INDEX idx_transactions_2025_06_transaction_id ON wallet_service.transactions_2025_06(transaction_id);
CREATE INDEX idx_transactions_2025_06_wallet_id ON wallet_service.transactions_2025_06(wallet_id);
CREATE INDEX idx_transactions_2025_06_created_at ON wallet_service.transactions_2025_06(created_at);

CREATE UNIQUE INDEX idx_transactions_2025_07_transaction_id ON wallet_service.transactions_2025_07(transaction_id);
CREATE INDEX idx_transactions_2025_07_wallet_id ON wallet_service.transactions_2025_07(wallet_id);
CREATE INDEX idx_transactions_2025_07_created_at ON wallet_service.transactions_2025_07(created_at);

CREATE UNIQUE INDEX idx_transactions_2025_08_transaction_id ON wallet_service.transactions_2025_08(transaction_id);
CREATE INDEX idx_transactions_2025_08_wallet_id ON wallet_service.transactions_2025_08(wallet_id);
CREATE INDEX idx_transactions_2025_08_created_at ON wallet_service.transactions_2025_08(created_at);

CREATE UNIQUE INDEX idx_transactions_2025_09_transaction_id ON wallet_service.transactions_2025_09(transaction_id);
CREATE INDEX idx_transactions_2025_09_wallet_id ON wallet_service.transactions_2025_09(wallet_id);
CREATE INDEX idx_transactions_2025_09_created_at ON wallet_service.transactions_2025_09(created_at);

CREATE UNIQUE INDEX idx_transactions_2025_10_transaction_id ON wallet_service.transactions_2025_10(transaction_id);
CREATE INDEX idx_transactions_2025_10_wallet_id ON wallet_service.transactions_2025_10(wallet_id);
CREATE INDEX idx_transactions_2025_10_created_at ON wallet_service.transactions_2025_10(created_at);

CREATE UNIQUE INDEX idx_transactions_2025_11_transaction_id ON wallet_service.transactions_2025_11(transaction_id);
CREATE INDEX idx_transactions_2025_11_wallet_id ON wallet_service.transactions_2025_11(wallet_id);
CREATE INDEX idx_transactions_2025_11_created_at ON wallet_service.transactions_2025_11(created_at);

CREATE UNIQUE INDEX idx_transactions_2025_12_transaction_id ON wallet_service.transactions_2025_12(transaction_id);
CREATE INDEX idx_transactions_2025_12_wallet_id ON wallet_service.transactions_2025_12(wallet_id);
CREATE INDEX idx_transactions_2025_12_created_at ON wallet_service.transactions_2025_12(created_at);

ALTER TABLE wallet_service.transactions ADD CONSTRAINT transactions_pk PRIMARY KEY (transaction_id, created_at);

-- Table for CQRS event store
CREATE TABLE wallet_service.events (
    event_id UUID NOT NULL,
    aggregate_id UUID NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    event_data TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    FOREIGN KEY (aggregate_id) REFERENCES wallet_service.wallets(wallet_id),
    PRIMARY KEY (event_id)
);

CREATE INDEX idx_events_aggregate_id ON wallet_service.events(aggregate_id);
CREATE INDEX idx_events_created_at ON wallet_service.events(created_at);
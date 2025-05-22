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

-- Table for wallets
CREATE TABLE wallet_service.wallets (
    wallet_id UUID NOT NULL,
    user_id UUID NOT NULL,
    currency VARCHAR(3) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    FOREIGN KEY (user_id) REFERENCES wallet_service.users(user_id),
    PRIMARY KEY (wallet_id)
);

-- Table for wallet balances
CREATE TABLE wallet_service.wallet_balances (
    wallet_id UUID NOT NULL,
    balance DECIMAL(19,2) NOT NULL,
    last_transaction_id UUID NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    FOREIGN KEY (wallet_id) REFERENCES wallet_service.wallets(wallet_id),
    PRIMARY KEY (wallet_id)
);

-- Table for transactions
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
    CONSTRAINT valid_type CHECK (transaction_type IN ('DEPOSIT', 'WITHDRAWAL', 'TRANSFER_SENT', 'TRANSFER_RECEIVED')),
    PRIMARY KEY (transaction_id)
);

-- Table for events
CREATE TABLE wallet_service.events (
    event_id UUID NOT NULL,
    aggregate_id UUID NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    event_data TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (event_id)
);
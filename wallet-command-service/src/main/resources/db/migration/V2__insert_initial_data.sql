-- Insert initial users with intuitive UUIDs
INSERT INTO wallet_service.users (user_id, username, email, created_at, updated_at)
VALUES
    ('550e8400-e29b-41d4-a716-446655440001', 'user1', 'user1@example.com', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
    ('550e8400-e29b-41d4-a716-446655440002', 'user2', 'user2@example.com', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

-- Insert initial wallets with intuitive UUIDs
INSERT INTO wallet_service.wallets (wallet_id, user_id, currency, created_at, updated_at)
VALUES
    ('550e8400-e29b-41d4-a716-446655440101', '550e8400-e29b-41d4-a716-446655440001', 'USD', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
    ('550e8400-e29b-41d4-a716-446655440102', '550e8400-e29b-41d4-a716-446655440002', 'USD', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

-- Insert initial wallet balances
INSERT INTO wallet_service.wallet_balances (wallet_id, balance, last_transaction_id, updated_at)
VALUES
    ('550e8400-e29b-41d4-a716-446655440101', 0.00, '00000000-0000-0000-0000-000000000000', CURRENT_TIMESTAMP),
    ('550e8400-e29b-41d4-a716-446655440102', 0.00, '00000000-0000-0000-0000-000000000000', CURRENT_TIMESTAMP);
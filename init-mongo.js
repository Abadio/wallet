db = db.getSiblingDB('wallet_service');
db.createCollection('wallet_balances');
db.createCollection('transaction_history');
db.createCollection('daily_balance');
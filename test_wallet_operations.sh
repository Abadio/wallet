#!/bin/bash

# Base URL for wallet-command-service
BASE_URL="http://localhost:8081/api/command/wallets"

# Ensure the database is reset to the initial state (optional, run manually if needed)
# docker exec -it mongodb mongosh wallet_service --eval "db.wallet_balances.drop(); db.wallets.drop(); db.users.drop();"
# Then reload the initial data using your SQL-to-MongoDB migration tool

# Test 1: Deposit $100 to wallet1
echo "Executing deposit of \$100 to wallet1 (550e8400-e29b-41d4-a716-446655440101)..."
curl -X POST "$BASE_URL/550e8400-e29b-41d4-a716-446655440101/deposit?amount=100&description=Initial%20deposit" \
     -H "Content-Type: application/json" \
     -w "\nHTTP Status: %{http_code}\n" \
     -s -o response.json
cat response.json
echo -e "\nWaiting for event propagation..."
sleep 2

# Test 2: Deposit $50 to wallet2
echo "Executing deposit of \$50 to wallet2 (550e8400-e29b-41d4-a716-446655440102)..."
curl -X POST "$BASE_URL/550e8400-e29b-41d4-a716-446655440102/deposit?amount=50&description=Initial%20deposit" \
     -H "Content-Type: application/json" \
     -w "\nHTTP Status: %{http_code}\n" \
     -s -o response.json
cat response.json
echo -e "\nWaiting for event propagation..."
sleep 2

# Test 3: Withdraw $30 from wallet1
echo "Executing withdrawal of \$30 from wallet1 (550e8400-e29b-41d4-a716-446655440101)..."
curl -X POST "$BASE_URL/550e8400-e29b-41d4-a716-446655440101/withdraw?amount=30&description=Withdrawal%20test" \
     -H "Content-Type: application/json" \
     -w "\nHTTP Status: %{http_code}\n" \
     -s -o response.json
cat response.json
echo -e "\nWaiting for event propagation..."
sleep 2

# Test 4: Transfer $20 from wallet1 to wallet2
echo "Executing transfer of \$20 from wallet1 (550e8400-e29b-41d4-a716-446655440101) to wallet2 (550e8400-e29b-41d4-a716-446655440102)..."
curl -X POST "$BASE_URL/transfer?fromWalletId=550e8400-e29b-41d4-a716-446655440101&toWalletId=550e8400-e29b-41d4-a716-446655440102&amount=20&description=Transfer%20test" \
     -H "Content-Type: application/json" \
     -w "\nHTTP Status: %{http_code}\n" \
     -s -o response.json
cat response.json
echo -e "\nWaiting for event propagation..."
sleep 2

# Clean up
rm response.json
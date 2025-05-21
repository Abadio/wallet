# Wallet Service

## Description

The **Wallet Service** is a digital wallet application that enables financial operations such as deposits, withdrawals, and transfers between wallets, as well as querying current balances, transaction histories, and daily balances. The application is built with a microservices architecture, leveraging the CQRS (Command Query Responsibility Segregation) pattern to separate write and read operations, ensuring scalability, consistency, and performance.

## Solution Architecture

The application consists of three microservices, each with distinct responsibilities:

1. **Wallet Command Service**:
   - Handles financial operations (deposit, withdrawal, transfer).
   - Stores data in a PostgreSQL database, including user information, wallets, balances, transactions, and events.
   - Publishes events (e.g., `DepositedEvent`, `WithdrawnEvent`, `TransferredEvent`) to the Kafka topic `wallet-events`.
   - Endpoint: `http://localhost:8081/api/command`.
   - Swagger: `http://localhost:8081/swagger-ui/index.html`.

2. **Wallet Query Service**:
   - Performs read queries, such as current balance, transaction history, and daily balance.
   - Retrieves data from a MongoDB database, optimized for read operations.
   - Endpoint: `http://localhost:8082/api/query`.
   - Swagger: `http://localhost:8082/swagger-ui/index.html`.

3. **Wallet Consumer Service**:
   - Consumes events from the Kafka topic `wallet-events`.
   - Updates MongoDB collections (`wallet_balances`, `transaction_history`, `daily_balance`) based on received events.
   - Ensures read projections are consistent with performed operations.

### Infrastructure

- **PostgreSQL**: Stores transactional data in the `wallet_service` database (tables `users`, `wallets`, `wallet_balances`, `transactions`, `events`). The `transactions` table, which records all financial flows and has the highest data volume, is partitioned by month (e.g., `transactions_2025_01`, `transactions_2025_02`) to optimize query and insertion performance, but users query the `transactions` table directly, as PostgreSQL abstracts the partitioning. Uses pessimistic locking to ensure balance consistency during concurrent operations.
- **MongoDB**: Database `wallet_service` with collections `wallet_balances`, `transaction_history`, and `daily_balance` for fast queries.
- **Kafka**: Manages asynchronous events in the topics `wallet-events` (main events) and `wallet-events-dlq` (error messages).

### Architectural Choices

- **CQRS**: Separates write operations (PostgreSQL) from read operations (MongoDB) to optimize performance and scalability. Write operations are handled by the `wallet-command-service`, while queries are managed by the `wallet-query-service`.
- **Pessimistic Locking in PostgreSQL**: Ensures balance consistency during concurrent operations, preventing race conditions when updating the `wallet_balances` table.
- **Event Sourcing with Kafka**: Events are published to Kafka for asynchronous propagation, enabling eventual consistency between write and read operations.
- **Partitioning of the `transactions` Table**: The `transactions` table in PostgreSQL is partitioned by month to improve performance, given that it stores all financial flows and has the highest data volume in the solution. PostgreSQL manages partitioning transparently, allowing queries directly on the `transactions` table.

## How to Start the Application

1. Ensure Docker and Docker Compose are installed.
2. Clone the repository:

   ```bash
   git clone https://github.com/Abadio/wallet.git
   cd wallet
   ```

3. Start all services (PostgreSQL, MongoDB, Kafka, Zookeeper, microservices):

   ```bash
   docker compose up -d --build
   ```

4. Wait until services are healthy (check with `docker ps`).

## How to Stop the Application

To stop and remove all containers:

```bash
docker compose down
```

To also remove volumes (persistent data):

```bash
docker compose down -v
```

## API Calls

The application provides two main APIs: one for financial operations (`wallet-command-service`) and another for queries (`wallet-query-service`).

### Initial Data

The initial data (defined in the SQL script) creates:
- Users: `user1` (`550e8400-e29b-41d4-a716-446655440001`), `user2` (`550e8400-e29b-41d4-a716-446655440002`).
- Wallets: `wallet1` (`550e8400-e29b-41d4-a716-446655440101`, user `user1`), `wallet2` (`550e8400-e29b-41d4-a716-446655440102`, user `user2`), both with an initial balance of $0.00.

### Wallet Command Service (Financial Operations)

**Base URL**: `http://localhost:8081/api/command`

#### Via cURL

- **Deposit $100 to `wallet1`**:

  ```bash
  curl -X POST "http://localhost:8081/api/command/wallets/550e8400-e29b-41d4-a716-446655440101/deposit?amount=100&description=Initial%20deposit" \
       -H "Content-Type: application/json"
  ```

- **Withdraw $30 from `wallet1`**:

  ```bash
  curl -X POST "http://localhost:8081/api/command/wallets/550e8400-e29b-41d4-a716-446655440101/withdraw?amount=30&description=Withdrawal%20test" \
       -H "Content-Type: application/json"
  ```

- **Transfer $20 from `wallet1` to `wallet2`**:

  ```bash
  curl -X POST "http://localhost:8081/api/command/wallets/transfer?fromWalletId=550e8400-e29b-41d4-a716-446655440101&toWalletId=550e8400-e29b-41d4-a716-446655440102&amount=20&description=Transfer%20test" \
       -H "Content-Type: application/json"
  ```

#### Via Swagger

- Access: `http://localhost:8081/swagger-ui/index.html`
- Available endpoints:
  - `POST /api/command/wallets/{walletId}/deposit`: Deposits funds into a wallet.
  - `POST /api/command/wallets/{walletId}/withdraw`: Withdraws funds from a wallet.
  - `POST /api/command/wallets/transfer`: Transfers funds between wallets.

### Wallet Query Service (Queries)

**Base URL**: `http://localhost:8082/api/query`

#### Via cURL

- **Query balance of `wallet1`**:

  ```bash
  curl -X GET "http://localhost:8082/api/query/wallets/550e8400-e29b-41d4-a716-446655440101/balance" \
       -H "Accept: application/json"
  ```

- **Query transaction history of `wallet1`**:

  ```bash
  curl -X GET "http://localhost:8082/api/query/wallets/550e8400-e29b-41d4-a716-446655440101/history" \
       -H "Accept: application/json"
  ```

- **Query daily balance of `wallet1` on 2025-05-21**:

  ```bash
  curl -X GET "http://localhost:8082/api/query/wallets/550e8400-e29b-41d4-a716-446655440101/historical-balance?date=2025-05-21" \
       -H "Accept: application/json"
  ```

#### Via Swagger

- Access: `http://localhost:8082/swagger-ui/index.html`
- Available endpoints:
  - `GET /api/query/wallets/{walletId}/balance`: Retrieves the current balance.
  - `GET /api/query/wallets/{walletId}/history`: Retrieves transaction history (last 30 days).
  - `GET /api/query/wallets/{walletId}/historical-balance`: Retrieves the balance on a specific date.

## Investigate Microservice Logs

To check the logs of each microservice:

- **Wallet Command Service**:

  ```bash
  docker logs wallet-command-service
  ```

- **Wallet Query Service**:

  ```bash
  docker logs wallet-query-service
  ```

- **Wallet Consumer Service**:

  ```bash
  docker logs wallet-consumer-service
  ```

To follow logs in real-time, add `--follow`:

```bash
docker logs wallet-command-service --follow
```

## Query Data in PostgreSQL

1. Access the `postgres` container:

   ```bash
   docker exec -it postgres psql -U postgres -d wallet_service
   ```

2. Example SQL queries:

   - **List users**:

     ```sql
     SELECT * FROM wallet_service.users;
     ```

   - **List wallets**:

     ```sql
     SELECT * FROM wallet_service.wallets;
     ```

   - **Query balance of `wallet1`**:

     ```sql
     SELECT * FROM wallet_service.wallet_balances WHERE wallet_id = '550e8400-e29b-41d4-a716-446655440101';
     ```

   - **Query transactions of `wallet1`**:

     ```sql
     SELECT * FROM wallet_service.transactions WHERE wallet_id = '550e8400-e29b-41d4-a716-446655440101';
     ```

3. Exit `psql`:

   ```sql
   \q
   ```

## Query Data in Kafka

1. Access the `kafka` container:

   ```bash
   docker exec -it kafka bash
   ```

2. Query topics `wallet-events` and `wallet-events-dlq`:

   - **List events in `wallet-events`**:

     ```bash
     kafka-console-consumer --bootstrap-server kafka:9092 --topic wallet-events --from-beginning
     ```

   - **List messages in `wallet-events-dlq`**:

     ```bash
     kafka-console-consumer --bootstrap-server kafka:9092 --topic wallet-events-dlq --from-beginning
     ```

3. Exit the container:

   ```bash
   exit
   ```

## Query Data in MongoDB

**Note**: The `wallet_balances`, `transaction_history`, and `daily_balance` collections may be empty after running the `reset-solution.sh` script. To populate the data, run the `test_wallet_operations.sh` script before performing queries.

1. Access the `mongodb` container:

   ```bash
   docker exec -it mongodb mongosh wallet_service
   ```

2. Example queries:

   - **Query balance of `wallet1`**:

     ```javascript
     db.wallet_balances.findOne({ "_id": UUID("550e8400-e29b-41d4-a716-446655440101") })
     ```

   - **Query transaction history of `wallet1`**:

     ```javascript
     db.transaction_history.find({ "walletId": UUID("550e8400-e29b-41d4-a716-446655440101") }).sort({ "createdAt": -1 })
     ```

   - **Query daily balance of `wallet1` on 2025-05-21**:

     ```javascript
     db.daily_balance.findOne({ "walletId": UUID("550e8400-e29b-41d4-a716-446655440101"), "date": "2025-05-21" })
     ```

3. Exit `mongosh`:

   ```javascript
   exit
   ```

## Run the Automated Operations Script

The `test_wallet_operations.sh` script performs a sequence of financial operations via cURL:

1. Make the script executable:

   ```bash
   chmod +x test_wallet_operations.sh
   ```

2. Run the script:

   ```bash
   ./test_wallet_operations.sh
   ```

3. The script will execute:
   - Deposit $100 to `wallet1`.
   - Deposit $50 to `wallet2`.
   - Withdraw $30 from `wallet1`.
   - Transfer $20 from `wallet1` to `wallet2`.

## Reset the Application

The `reset-solution.sh` script clears all data (PostgreSQL, MongoDB, Kafka) and restarts the services:

1. Make the script executable:

   ```bash
   chmod +x reset-solution.sh
   ```

2. Run the script:

   ```bash
   ./reset-solution.sh
   ```

3. This will:
   - Stop all containers.
   - Clear PostgreSQL and MongoDB databases.
   - Recreate Kafka topics.
   - Restart services with the initial data.
# Quick Start Guide

This guide will help you get started with the Verifiable Postgres Proxy.

## Prerequisites

- Go 1.21 or later
- Docker and Docker Compose (for easy setup)
- PostgreSQL client (psql)

## Getting Started with Docker

The easiest way to get started is using Docker Compose, which will set up:
1. A backend PostgreSQL database
2. A verification PostgreSQL database
3. The Verifiable Postgres Proxy

```bash
# Start the stack
make docker-up

# Or to see logs in real-time
make docker-up-logs
```

The proxy will be available on port 5432.

## Connecting to the Proxy

Once the stack is running, you can connect to the proxy using any PostgreSQL client:

```bash
psql -h localhost -p 5432 -U postgres -d postgres
```

When prompted for a password, enter `postgres`.

## Testing the Proxy

Try running some SQL commands:

```sql
-- Create a table
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert some data
INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com');
INSERT INTO users (name, email) VALUES ('Bob', 'bob@example.com');

-- Query the data
SELECT * FROM users;

-- Update data
UPDATE users SET name = 'Alice Smith' WHERE id = 1;

-- Delete data
DELETE FROM users WHERE id = 2;
```

## Viewing Verification Results

The proxy logs contain information about state commitments and verification results. You can view them:

```bash
# If running with docker-up-logs, you'll see them in real-time
# Otherwise, you can check the logs with:
docker-compose logs proxy
```

Look for log entries with these messages:
- "State Commitment Record" - Shows the Merkle roots before and after a transaction
- "Verification Result" - Shows whether the verification succeeded or failed

## Building and Running Locally

If you'd prefer to run the components locally:

```bash
# Build the proxy
make build

# Run the proxy
./verifiable-postgres-proxy \
  --listen=localhost:5432 \
  --backend-host=localhost \
  --backend-port=5433 \
  --verification-host=localhost \
  --verification-port=5434
```

Note: You'll need to have PostgreSQL instances running on ports 5433 and 5434 for this to work.

## Shutting Down

To stop the Docker Compose stack:

```bash
make docker-down
```
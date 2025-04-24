# Verifiable Postgres Proxy

A proxy server that sits between PostgreSQL clients and a PostgreSQL database, enabling verification of query execution through deterministic replay.

## Overview

The Verifiable Postgres Proxy intercepts SQL queries, captures state before and after execution, calculates cryptographic commitments, and verifies query execution through deterministic replay. This architecture enables trustless verification of database state transitions.

## Features

- Intercepts and forwards PostgreSQL wire protocol messages
- Captures pre/post state of database tables
- Generates cryptographic commitments (Merkle roots) of database state
- Asynchronously verifies state transitions through deterministic replay
- Logs warnings for non-deterministic functions

## Components

- **Proxy Service**: Handles PostgreSQL protocol and client connections
- **Query Interceptor**: Analyzes SQL queries to determine type and affected tables
- **State Capture**: Captures table state before and after query execution
- **Commitment Generator**: Creates Merkle tree roots from table state
- **Replay Engine**: Verifies state transitions through deterministic replay

## Building and Running

### Prerequisites

- Go 1.21 or later
- PostgreSQL 13 or later (two instances: backend and verification)

### Build

```bash
go build -o verifiable-postgres-proxy ./cmd/proxy
```

### Run

```bash
./verifiable-postgres-proxy \
  --listen=localhost:5432 \
  --backend-host=localhost \
  --backend-port=5433 \
  --verification-host=localhost \
  --verification-port=5434
```

### Configuration

The proxy accepts various configuration options as command-line flags:

- `--listen`: Address to listen on (default: localhost:5432)
- `--backend-host`: Backend database host (default: localhost)
- `--backend-port`: Backend database port (default: 5433)
- `--verification-host`: Verification database host (default: localhost)
- `--verification-port`: Verification database port (default: 5434)
- `--worker-pool-size`: Number of verification workers (default: 4)
- `--enable-verification`: Enable verification (default: true)
- `--log-level`: Log level (default: info)

## Limitations

This is an MVP (Minimum Viable Product) with the following limitations:

- Simple Query Protocol only (no Extended Protocol support)
- SELECT-based state capture (vulnerable to race conditions)
- Only warns about non-deterministic functions (no query rewriting)
- Limited transaction support
- No on-chain integration or cryptographic proofs
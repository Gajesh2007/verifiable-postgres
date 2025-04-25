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
- Robust SQL parsing with full support for complex queries, joins, and subqueries
- Deterministic RowID generation for consistent Merkle roots
- Atomic post-state capture within transactions for accuracy
- Metrics exposed via Prometheus and JSON endpoints
- Configurable via environment variables, config files, and command line flags

## Components

- **Proxy Service**: Handles PostgreSQL protocol and client connections
- **Query Interceptor**: Analyzes SQL queries to determine type and affected tables
- **State Capture**: Captures table state before and after query execution
- **Commitment Generator**: Creates Merkle tree roots from table state
- **Replay Engine**: Verifies state transitions through deterministic replay
- **Metrics Server**: Exposes operational metrics via HTTP endpoints

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
./verifiable-postgres-proxy
```

### Configuration

The proxy can be configured in three ways (in order of precedence):

1. Command-line flags
2. Environment variables
3. Configuration file

#### Configuration File

Create a YAML configuration file based on the provided `config.example.yaml`:

```bash
cp config.example.yaml config.yaml
# Edit config.yaml with your settings
./verifiable-postgres-proxy
```

#### Environment Variables

All configuration options can be set via environment variables with the `VPG_` prefix. Nested keys use underscores:

```bash
export VPG_PROXY_LISTEN_ADDR=localhost:5432
export VPG_BACKEND_HOST=localhost
export VPG_BACKEND_PORT=5433
./verifiable-postgres-proxy
```

#### Command-line Flags

For quick configuration, command-line flags can be used:

```bash
./verifiable-postgres-proxy \
  --proxy.listen_addr=localhost:5432 \
  --backend.host=localhost \
  --backend.port=5433 \
  --verification.host=localhost \
  --verification.port=5434 \
  --replay.worker_pool_size=4 \
  --features.enable_verification=true \
  --log.level=info
```

See `config.example.yaml` for a complete list of configuration options.

## Metrics

Metrics are exposed via HTTP endpoints:

- Prometheus metrics: `http://localhost:2112/metrics/prometheus`
- JSON metrics: `http://localhost:2112/metrics/json`

The metrics port can be configured using the `proxy.metrics_addr` setting.

## V1 Limitations

This version has several known limitations that will be addressed in future versions:

1. **Pre-State Capture Race Condition**: The pre-state capture occurs outside the backend transaction, allowing for race conditions between pre-state capture and DML execution. This is a fundamental limitation of the SELECT-based capture approach in V1.

2. **Full Table Capture Inefficiency**: State capture performs `SELECT * FROM table` for every affected table, which is inefficient for large tables. Future versions will use WAL/logical decoding to capture only changed rows.

3. **Simple Query Protocol Only**: The proxy only supports the Simple Query Protocol. The Extended Query Protocol with prepared statements is not supported in V1.

4. **Transaction Support Limitations**: While the proxy supports basic transactions, it has limitations with complex transaction patterns and isolation levels.

5. **No On-Chain Integration**: The cryptographic commitments are not yet integrated with any blockchain or external verification system.

## Future Work (V2)

The following improvements are planned for V2:

1. **WAL-Based State Capture**: Replace SELECT-based state capture with Write-Ahead Log (WAL) processing using logical decoding. This will eliminate race conditions and provide accurate pre/post images without any performance impact.

2. **Extended Query Protocol Support**: Add support for the PostgreSQL Extended Query Protocol (prepared statements, binary format, parameter binding).

3. **On-Chain Integration**: Integrate with blockchain networks to publish state commitments as cryptographic proofs.

4. **Query Rewriting**: Automatically rewrite non-deterministic functions to make them deterministic.

5. **Performance Optimizations**: Implement connection pooling, caching, and other performance improvements.

6. **Multi-Schema Support**: Enhance support for multiple schemas and cross-schema operations.

## Architecture

```
┌─────────────┐     ┌──────────────────────────────────┐     ┌────────────────┐
│ PostgreSQL  │     │ Verifiable Postgres Proxy        │     │ PostgreSQL     │
│ Client      │◄────┤                                  │◄────┤ Backend DB     │
└─────────────┘     │  ┌─────────┐  ┌───────────────┐  │     └────────────────┘
                    │  │ Query   │  │ State Capture │  │
                    │  │ Analysis│  │ & Commitments │  │
                    │  └─────────┘  └───────────────┘  │
                    │                      │           │
                    └──────────────────────┼───────────┘
                                           ▼
                    ┌──────────────────────────────────┐     ┌────────────────┐
                    │ Replay Engine                    │     │ PostgreSQL     │
                    │ (Asynchronous Verification)      │◄────┤ Verification DB│
                    └──────────────────────────────────┘     └────────────────┘
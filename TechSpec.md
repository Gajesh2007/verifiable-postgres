Let's integrate deterministic replay verification into the V1 MVP spec. EigenLayer/DA integration remains deferred.

---

**Tech Spec: Verifiable Postgres Proxy (Go) - V1 MVP (with Deterministic Replay)**

**1. Introduction & Goals**

1.1.  **Project:** Verifiable RDS AVS - Go Proxy V1
1.2.  **Goal:** To create a proxy server, written in Go, that sits between a PostgreSQL client and a standard PostgreSQL database. The proxy will intercept queries, capture necessary pre/post state information, calculate cryptographic state commitments (Merkle roots), and trigger an asynchronous deterministic replay process to verify the validity of the calculated state transition against the executed queries.
1.3.  **V1 Scope (MVP):**
    *   Implement a PostgreSQL wire protocol proxy handling basic client connections, authentication (simplified: Trust/Cleartext), and simple query (Query message type) forwarding.
    *   Intercept SQL queries.
    *   Parse and analyze queries to determine type (SELECT, INSERT, UPDATE, DELETE) and identify accessed tables.
    *   Detect and **log warnings** for known non-deterministic function usage (e.g., `NOW()`, `RANDOM()`). **No query rewriting or blocking in V1.**
    *   Implement a **simplified state capture mechanism** (e.g., querying affected rows before/after DML) for tables involved in INSERT, UPDATE, DELETE operations.
    *   Generate Merkle roots for affected tables based on captured state.
    *   Generate an overall database state root (Merkle root of table roots) representing the *claimed* post-transaction state (`PostRootClaimed`).
    *   **Log** the transaction details: `StateCommitmentRecord { TxID, QuerySequence, PreRootCaptured, PostRootClaimed, PreStateData }`.
    *   Implement an **asynchronous `ReplayEngine`**:
        *   Receives verification jobs (`TxID, QuerySequence, PreStateData, PreRootCaptured`).
        *   Sets up an isolated Postgres environment.
        *   Restores the necessary pre-state using `PreStateData`.
        *   Deterministically executes the `QuerySequence`.
        *   Captures the resulting state using the *same* state capture logic as the proxy.
        *   Calculates the resulting state root (`PostRootCalculated`).
        *   Compares `PostRootCalculated` with the `PostRootClaimed` (retrieved via TxID).
        *   **Logs** the verification result: `VerificationResult { TxID, Success: bool, PreRootCaptured, PostRootClaimed, PostRootCalculated, Mismatches: Option<...> }`.
    *   **Out of Scope for V1:** Complex query analysis/rewriting (beyond warnings), extended query protocol support, on-chain contract interaction (EigenLayer/AVS), challenge mechanisms, EigenDA integration, high availability, advanced security features, public verification APIs.

1.4.  **Success Criteria (V1):**
    *   Proxy successfully forwards basic SQL CRUD queries.
    *   Proxy logs `StateCommitmentRecord` for DML transactions.
    *   Proxy logs warnings for non-deterministic functions.
    *   `ReplayEngine` asynchronously processes verification jobs.
    *   `ReplayEngine` logs `VerificationResult` indicating success or failure by comparing the proxy's claimed post-state root with the deterministically replayed post-state root.
    *   System is stable under moderate load for simple queries.

**2. High-Level Architecture (Go)**

```
+-------------+      +-----------------------------+      +-----------------+
|   PSQL      |<---->|  Go Proxy Service           |<---->|   Backend       |
|   Client    |      | (Handles Connections, Proto)|      |   Postgres DB   |
+-------------+      +-----------------------------+      +-----------------+
                          |        ^         | Log StateCommitmentRecord
                          |        | Forward | + Send Job Async
                          | Intercept        v                 v
+---------------------+--------+---------------------+   +-----------------+
| Query Interceptor   |        | State Capture       |   |   Job Queue     |
| (SQL Parser)        |------->| Service             |-->| (e.g., Channel) |
|                     | Analyzes | (DB Interaction)    |   +-----------------+
+---------------------+--------+---------------------+                  | Receive Job Async
                          |                             Calculates Roots |
                          v                                              v
                  +---------------------+                    +--------------------+
                  | Commitment          |                    |  Replay Engine     |
                  | Generator           |<-------------------| (Async Worker)     |
                  | (Merkle Tree Logic) | Uses Logic         | (Manages Verif DB) |
                  +---------------------+                    +--------------------+
                                                                 | Logs VerificationResult
```

*   **Go Proxy Service:** Handles client/backend connections and protocol. Orchestrates interception, capture, commitment logging, and *asynchronously* sends verification jobs.
*   **Query Interceptor:** Parses SQL, identifies type, tables, non-deterministic patterns (logs warnings). Tracks query sequence within a transaction.
*   **State Capture Service:** Used by *both* Proxy (for pre/post data) *and* Replay Engine (for post-replay data). Uses SELECTs against the respective DB (Backend for Proxy, Verification for Replay) based on QueryInfo.
*   **Commitment Generator:** Shared logic used by Proxy and Replay Engine to calculate table/db Merkle roots from captured `TableState`.
*   **Job Queue:** An in-memory channel (or potentially Redis/Kafka later) for sending verification tasks from the Proxy to the Replay Engine.
*   **Replay Engine:** Runs asynchronously. Gets jobs, sets up its *own isolated Postgres environment*, restores pre-state, executes queries deterministically, captures resulting state, calculates root, compares, and logs the verification outcome.

**3. Component Breakdown & Sequencing (Go)**

**(Sequence 1: Foundation)**

3.1.  **Project Setup & Core Types (`cmd/proxy/main.go`, `pkg/types/types.go`, `pkg/config/config.go`, `pkg/log/log.go`)**
    *   **Task:** Set up Go module, basic CLI flags (using `flag` or `cobra`), configuration loading (simple struct with defaults first, maybe Viper later), structured logging setup (`log/slog`).
    *   **Instructions:**
        *   Define core structs in `pkg/types/`: `Value`, `RowId`, `Row`, `TableState`, `QueryType` (enum), `QueryInfo`, `StateCommitmentRecord`, `VerificationJob`, `VerificationResult`, `TableSchema`, `ColumnDefinition`. Ensure they align with the *unified* logic needed.
        *   Define `Config` struct in `pkg/config/` holding parameters like listen/backend addresses, DB credentials for *both* backend and *verification* DB, V1 feature flags.
        *   Setup `slog` for structured JSON logging.
    *   **Reasoning:** Establishes the basic project structure, shared data definitions, configuration handling, and essential logging.

3.2.  **Commitment Generation Logic (`pkg/commitment/merkle.go`, `pkg/commitment/generator.go`)**
    *   **Task:** Implement the core hashing and Merkle tree logic.
    *   **Instructions:**
        *   Create helper functions for SHA-256 hashing with domain separation (e.g., `hashWithDomain(domain []byte, data ...[]byte) [32]byte`). Use constants for domains (`LeafDomain = []byte("LEAF:")`, etc.).
        *   Implement `BuildMerkleTree(leafData [][]byte) ([32]byte, error)`. Ensure deterministic input order (caller responsibility). Handle padding for odd leaves correctly.
        *   Implement `GenerateTableRoot(tableState *types.TableState) ([32]byte, error)`. This function should:
            *   Get rows from `tableState.Rows`.
            *   Sort rows deterministically (e.g., based on `RowId`).
            *   Serialize each row deterministically (e.g., sort `Values` map keys, use stable JSON marshal).
            *   Hash each serialized row using `hashWithDomain`.
            *   Call `BuildMerkleTree` with the row hashes.
        *   Implement `GenerateDatabaseRoot(tableRoots map[string][32]byte) ([32]byte, error)`. This function should:
            *   Get table names from the map keys.
            *   Sort table names deterministically.
            *   For each table name, create leaf data `hashWithDomain(TableDomain, []byte(tableName), tableRoots[tableName][:])`.
            *   Call `BuildMerkleTree` with these table leaf hashes.
        *   Add extensive unit tests for hashing and Merkle tree construction with various inputs.
    *   **Reasoning:** Provides the fundamental cryptographic building blocks needed by both the proxy and the replay engine. Must be correct and deterministic.

**(Sequence 2: Basic Proxying)**

3.3.  **Proxy Service - Core Networking (`pkg/proxy/server.go`, `pkg/proxy/connection.go`)**
    *   **Task:** Implement the TCP server and basic PostgreSQL protocol handling for connection setup and teardown.
    *   **Instructions:**
        *   Use `net.Listen` to accept TCP connections.
        *   Use `go handleClient(conn)` for concurrency.
        *   Implement the PG Startup Flow: Read StartupMessage, send AuthenticationRequest (V1: Trust or CleartextPassword), handle PasswordMessage if needed, send AuthenticationOk, ParameterStatus (hardcoded essential ones like `server_version`, `client_encoding`), BackendKeyData (dummy values), ReadyForQuery. Use `jackc/pgx/v5/pgproto3` for message reading/writing.
        *   Implement basic backend connection logic: establish one backend connection per client connection using `database/sql` + `pgx`.
        *   Handle Terminate message.
        *   Implement basic error handling (send ErrorResponse to client).
    *   **Reasoning:** Establishes the ability to accept client connections and communicate using the basic PG protocol, setting the stage for query interception.

**(Sequence 3: Interception & V1 State Capture)**

3.4.  **Query Interceptor (`pkg/interceptor/analyzer.go`)**
    *   **Task:** Implement SQL parsing and basic analysis.
    *   **Instructions:**
        *   Choose a Go SQL parser (`vitess/go/vt/sqlparser` recommended).
        *   Implement `AnalyzeQuery(sql string) (*types.QueryInfo, error)`.
        *   Parse the SQL. Handle parse errors.
        *   Identify query type (SELECT, INSERT, UPDATE, DELETE -> `types.QueryType`). Treat others as `OTHER`.
        *   Extract table names from simple DML/SELECT statements using AST traversal. Handle basic aliases.
        *   Perform simple string checking for the list of non-deterministic functions (`NOW()`, `RANDOM()`, etc.). Set `IsDeterministic` flag and `Warning` message in `QueryInfo`.
        *   Add unit tests with various simple queries and non-deterministic functions.
    *   **Reasoning:** Enables the proxy to understand *what* the client is trying to do, which tables are involved, and if there are obvious deterministic issues (for logging).

3.5.  **State Capture Service (V1 - SELECT Based) (`pkg/capture/capture.go`)**
    *   **Task:** Implement the simplified state capture using `SELECT`.
    *   **Instructions:**
        *   Define `CapturePreState(queryInfo *types.QueryInfo, db *sql.DB) (map[string][]types.Row, error)`. This needs to parse the `WHERE` clause (simplification: maybe only capture based on primary key if WHERE is complex) and SELECT affected rows *before* the DML runs on the backend.
        *   Define `CapturePostState(queryInfo *types.QueryInfo, db *sql.DB) (map[string][]types.Row, error)`. Similar to pre-state, but runs *after* the DML completes on the backend.
        *   These functions need to dynamically build `SELECT` queries based on `queryInfo.AffectedTables` and potentially parsed WHERE clauses/primary keys. **Use parameterized queries or rigorously validate table/column names to prevent SQL injection.**
        *   Handle data type conversion from `database/sql` results into the `types.Value` representation.
    *   **Reasoning:** Provides the necessary (though simplified) state data required for commitment generation in V1.

3.6.  **Proxy Service - Integration (`pkg/proxy/connection.go`)**
    *   **Task:** Integrate Interceptor, Capture, and Commitment Generator into the proxy's query handling loop.
    *   **Instructions:**
        *   When a `Query` message arrives:
            *   Call `interceptor.AnalyzeQuery`. Log warnings from `QueryInfo`.
            *   If DML:
                *   Call `capture.CapturePreState` against the backend DB.
                *   Generate pre-state table roots & overall pre-state root using `commitment` package logic. Store `PreRootCaptured` and `PreStateData`.
            *   Forward the original query to the backend DB.
            *   Receive result/CommandComplete from backend.
            *   If DML:
                *   Call `capture.CapturePostState` against the backend DB.
                *   Generate post-state table roots & overall post-state root (`PostRootClaimed`) using `commitment` package.
                *   Log the `types.StateCommitmentRecord` containing TxID (use an internal counter), query sequence (just the single query for V1 simple protocol), `PreRootCaptured`, `PostRootClaimed`, and potentially a reference to or summary of `PreStateData`.
                *   Create a `types.VerificationJob` containing `TxID, QuerySequence, PreStateData, PreRootCaptured, PostRootClaimed`.
                *   Send the job to the `ReplayEngine`'s queue (implement a simple channel for V1).
            *   Forward backend response(s) to the client.
    *   **Reasoning:** Connects the components to perform the core V1 loop: intercept, capture pre-state, execute, capture post-state, calculate roots, log commitment, and dispatch verification job.

**(Sequence 4: Deterministic Replay Verification)**

3.7.  **Replay Engine Setup (`pkg/replay/engine.go`)**
    *   **Task:** Set up the asynchronous worker, job queue, and verification DB connection management.
    *   **Instructions:**
        *   Create a struct `ReplayEngine` holding config, a reference to the shared `StateCaptureService`, `CommitmentGenerator` logic, and the verification DB connection string.
        *   Implement `Start()` method that launches a pool of worker goroutines.
        *   Use a buffered Go channel as the V1 job queue (`chan types.VerificationJob`).
        *   Each worker goroutine:
            *   Listens on the job channel.
            *   Establishes its *own connection* to the *verification* database (`database/sql` + `pgx`). **Crucially, this DB must be isolated and reset between jobs.**
            *   Processes the received job.
    *   **Reasoning:** Creates the asynchronous infrastructure for performing verification without blocking the main proxy loop.

3.8.  **Deterministic Execution Logic (`pkg/replay/engine.go`, `pkg/replay/deterministic.go`)**
    *   **Task:** Implement the logic to execute queries deterministically within the replay engine.
    *   **Instructions:**
        *   Implement `SetupVerificationDB(tx *sql.Tx, schema *types.TableSchema, preStateData []types.Row) error`. This function should `CREATE SCHEMA IF NOT EXISTS`, `DROP TABLE IF EXISTS`, `CREATE TABLE` using the schema, and `INSERT` the `preStateData`. Run this within a DB transaction.
        *   Implement `SetDeterministicSessionParams(tx *sql.Tx) error`. Set parameters like `timezone='UTC'`, `max_parallel_workers_per_gather = 0`, `enable_hashjoin = off` etc., inside the transaction.
        *   Implement `ExecuteQueriesDeterministically(tx *sql.Tx, queries []string) error`. Execute the sequence of queries *within the same DB transaction*. **V1 does not replace functions like `NOW()`, but the session params help.**
    *   **Reasoning:** Ensures the replay environment is clean and configured for maximum determinism before executing the queries.

3.9.  **Replay Engine - Verification Flow (`pkg/replay/engine.go`)**
    *   **Task:** Implement the full verification logic within the worker goroutine.
    *   **Instructions:**
        *   For each received `VerificationJob`:
            *   Start a DB transaction on the verification DB connection.
            *   Call `SetupVerificationDB` using `job.PreStateData` and the relevant table schema (needs to be accessible, maybe include in job or fetch). Handle errors.
            *   Call `SetDeterministicSessionParams`. Handle errors.
            *   Call `ExecuteQueriesDeterministically` with `job.QuerySequence`. Handle errors.
            *   If execution succeeds:
                *   Call `capture.CapturePostState` against the *verification* DB to get the resulting state.
                *   Use `commitment` logic to calculate `PostRootCalculated`.
                *   Compare `PostRootCalculated` with `job.PostRootClaimed`.
                *   Log the `types.VerificationResult` (success or failure, include both roots).
                *   `ROLLBACK` the verification DB transaction (to clean up for the next job).
            *   If any step fails:
                *   Log `types.VerificationResult` with `Success: false` and the error message.
                *   Ensure `ROLLBACK` is called on the verification DB transaction.
    *   **Reasoning:** Completes the verification loop by replaying the transaction under controlled conditions and comparing the outcome to what the proxy claimed.

**(Sequence 5: Testing & Refinement)**

3.10. **Integration & End-to-End Testing (`tests/`)**
    *   **Task:** Write tests that simulate client interaction, trigger verification, and check logs.
    *   **Instructions:**
        *   Set up a test environment with a backend DB and a verification DB.
        *   Run the proxy and replay engine.
        *   Use a Go PG client (`jackc/pgx/v5`) to send queries (INSERT, UPDATE, DELETE) to the proxy.
        *   Monitor the structured logs for both `StateCommitmentRecord` and `VerificationResult`.
        *   Assert that the `Success` flag in `VerificationResult` is `true` for matching states and `false` if you intentionally create a mismatch (e.g., by modifying the backend DB state between pre/post capture in the proxy *during the test only*).
        *   Test scenarios with non-deterministic function warnings.
    *   **Reasoning:** Validates that the entire system works together as expected.

**4. Configuration (`config/config.go`)**

*   Define a `Config` struct loaded via flags or file (e.g., using Viper).
*   Include sections for:
    *   Proxy (`ListenAddr`, `MaxConnections`)
    *   Backend DB (`Host`, `Port`, `User`, `Password`, `DBName`)
    *   Verification DB (`Host`, `Port`, `User`, `Password`, `DBName`)
    *   Replay Engine (`WorkerPoolSize`)
    *   Logging (`Level`, `Format`)
    *   V1 Feature Flags (`EnableVerification`, `LogNonDeterministicWarnings`)

**5. Security Considerations (V1 Specific)**

*   **Verification DB Security:** The verification DB must be strictly isolated. The proxy should *never* write to it directly. Only the Replay Engine should interact with it. Credentials should be separate from the backend DB.
*   **State Capture Accuracy:** The V1 SELECT-based capture is vulnerable to race conditions if concurrent transactions modify the same rows between the pre/post SELECTs. Acknowledge this limitation.
*   **Deterministic Functions:** V1 only warns. Applications using non-deterministic functions will likely cause verification failures in the replay engine. This needs to be communicated.

**6. Performance Goals (V1)**

*   Proxy Latency (SELECT): < 5ms (p95)
*   Proxy Latency (DML - including logging commitment): < 50ms (p95)
*   Replay Engine Verification Latency (per simple transaction): < 500ms (p95) - *Note: This is highly dependent on DB setup/replay complexity.*
*   Replay Engine Throughput: Target handling at least 10-50 verifiable transactions per second.

**7. Limitations & V2+ Roadmap**

*   **Verification Scope:** Only verifies state root match based on deterministic replay. Does not verify cryptographic proofs or involve on-chain components.
*   **State Capture:** V1 uses inefficient/less robust SELECT-based capture. V2 must implement WAL/Logical Decoding.
*   **Query Rewriting:** V1 only warns. V2 needs automated rewriting or blocking of non-deterministic queries.
*   **Protocol Support:** V1 only handles Simple Query Protocol. V2 needs Extended Protocol.
*   **Transaction Complexity:** V1 assumes simple transactions. V2 needs full savepoint handling, complex transaction modes.
*   **Error Handling:** V1 is basic. V2 needs more nuanced error reporting and recovery.
*   **Scalability:** V1 uses basic concurrency. V2 needs optimized pooling, potentially distributed replay workers.
*   **Security:** V1 focuses on core logic. V2 needs DoS protection, rate limiting, anomaly detection, etc.
*   **Observability:** V1 uses basic logging. V2 needs comprehensive metrics and tracing.
*   **Deployment:** V1 assumes simple deployment. V2 needs HA, proper configuration management.
*   **EigenLayer/DA:** Deferred entirely.

---

This detailed spec provides a concrete plan for V1, incorporating the critical deterministic replay verification while acknowledging the necessary simplifications (like state capture) and clearly deferring more advanced features and on-chain integration. Follow the sequencing for a structured implementation process.
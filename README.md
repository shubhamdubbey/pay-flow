# PayFlow — Production-Grade Payments & Wallet Platform

**Stack:** Java 17 · Spring Boot 3.x · Kafka · RabbitMQ · PostgreSQL · Redis · Docker Compose
**Purpose:** A hands-on, production-grade system for learning Kafka (all domain operations) and RabbitMQ (customer email delivery only) the way real fintech backends use them.

---

## 1. What the Application Looks Like

PayFlow is a mini digital-payments platform, similar in spirit to a UPI app plus a wallet.

### 1.1 What a user can do

| Capability | Description |
|---|---|
| **Create wallet account** | Every customer gets a wallet account with a unique VPA (e.g. `shubham@payflow`). |
| **Top-up wallet** | Money comes in from an external bank (simulated). |
| **Wallet → Wallet transfer** | Instant P2P transfer between two PayFlow accounts. |
| **UPI-style payment to external bank** | Pay to a VPA hosted at an external bank (e.g. `merchant@somebank`), routed through the Bank Gateway. |
| **Check balance** | Available balance vs. held (reserved) balance. |
| **View statement** | Paginated transaction history and daily summaries. |
| **Get notified** | Email on payment success, failure and reversal. |

### 1.2 A day in the life of one payment

Riya pays ₹500 to a coffee shop's VPA `cafe@somebank`:

1. Her app calls `POST /payments` with an `Idempotency-Key`. Payment Service stores the payment as `INITIATED`, answers `202 Accepted` with a `paymentId`, and records an event in its **outbox**.
2. The event reaches Kafka. **Fraud Service** scores it (amount, velocity, new-payee, odd hours) and emits `APPROVED`.
3. **Ledger Service** places a **hold** of ₹500 on Riya's account. Her *available* balance drops, but the money has not left yet.
4. **Bank Gateway** (simulator) is asked to move ₹500 to `cafe@somebank`. It may succeed, fail, time out or answer twice. The system must be correct in every case.
5. On success, Ledger **captures** the hold and posts double-entry records (debit Riya, credit the bank settlement account).
6. Payment becomes `COMPLETED`. **Statement Service** updates her statement. **Notification Service** pushes an email task into **RabbitMQ**, where a worker sends the mail with retries.
7. If anything fails midway, the saga **compensates**: the hold is released and Riya gets a failure email. No money is ever lost or created.

The application's defining property: **every money movement is traceable, idempotent, and reconcilable**, even when services crash, networks time out, or messages are duplicated.

---

## 2. Goals, Non-Goals and Quality Targets

### Goals
- Eventual consistency across services using sagas and the transactional outbox.
- Strong consistency inside each service (ACID in its own database).
- Never lose or double-apply a money movement.
- Observable, testable, deployable locally with one `docker compose up`.

### Non-Goals (for now)
- Real bank integration, real KYC, regulatory reporting.
- Multi-currency / FX (single currency `INR`, amounts in **paise** as `BIGINT`).
- Kubernetes manifests (can be a later phase).

### Non-Functional Targets
| Area | Target |
|---|---|
| Availability of payment initiation | Accept requests even if downstream services are briefly down (async). |
| Durability | Zero acknowledged payment lost (outbox + `acks=all`). |
| Idempotency | Same `Idempotency-Key` always returns the same result. |
| Ordering | Events for one account are processed in order. |
| Latency | P95 initiation < 200 ms; end-to-end completion typically seconds. |
| Recoverability | Any consumer can be restarted or replayed without side effects. |

---

## 3. High-Level Design

### 3.1 Component diagram

```
                              ┌──────────────┐
          Client ───HTTPS───▶ │ API Gateway  │  JWT validation, rate limiting (Redis token bucket),
                              │ (Spring Cloud│  correlation-id injection
                              │   Gateway)   │
                              └──────┬───────┘
                                     │ REST
          ┌──────────────────────────┼───────────────────────────────┐
          ▼                          ▼                               ▼
 ┌─────────────────┐      ┌──────────────────┐             ┌──────────────────┐
 │ Account/Query   │      │ Payment Service  │             │ Statement Service│
 │ (part of Ledger │      │  (Saga           │             │  (CQRS read      │
 │  REST API)      │      │   orchestrator)  │             │   model + API)   │
 └────────┬────────┘      └───────┬──────────┘             └────────▲─────────┘
          │                       │ outbox                          │
          │                       ▼                                 │
          │      ╔═══════════════════════════════════════════════════════════╗
          │      ║                      APACHE KAFKA                         ║
          │      ║ payment.events  ledger.commands  ledger.events            ║
          │      ║ fraud.events    gateway.commands gateway.events           ║
          │      ║ notification.requests   (+ <topic>.DLT dead-letter topics)║
          │      ╚══════╤════════════╤═══════════════╤═══════════════╤═══════╝
          │             │            │               │               │
          ▼             ▼            ▼               ▼               ▼
   ┌────────────┐ ┌────────────┐ ┌──────────────┐ ┌──────────────┐ ┌────────────────┐
   │  Ledger    │ │   Fraud    │ │ Bank Gateway │ │  Statement   │ │ Notification   │
   │  Service   │ │  Service   │ │  Simulator   │ │  consumer    │ │   Service      │
   │ (accounts, │ │ (rules +   │ │ (timeouts,   │ │              │ │ Kafka consumer │
   │ holds,     │ │  Redis     │ │  dupes,      │ │              │ │       │        │
   │ double-    │ │  velocity) │ │  delays)     │ │              │ │       ▼        │
   │ entry)     │ │            │ │              │ │              │ │ ┌────────────┐ │
   └─────┬──────┘ └─────┬──────┘ └──────┬───────┘ └──────┬───────┘ │ │ RabbitMQ   │ │
         │              │               │                │         │ │ email queue│ │
         ▼              ▼               ▼                ▼         │ │ retry/DLQ  │ │
    ┌─────────┐   ┌─────────┐     ┌─────────┐      ┌─────────┐     │ └─────┬──────┘ │
    │ledger_db│   │fraud_db │     │gateway_ │      │stmt_db  │     │       ▼        │
    │(Postgres)   │(Postgres)     │db       │      │(Postgres)     │ Email worker ─▶ SMTP
    └─────────┘   └─────────┘     └─────────┘      └─────────┘     └────────────────┘

    Payment Service ─▶ payment_db (Postgres)          Redis: idempotency fast-path,
                                                      rate limits, VPA cache, fraud counters
```

### 3.2 Why this split

| Service | Owns | Reason it is separate |
|---|---|---|
| **Payment** | Payment lifecycle, saga state, idempotency | Orchestrates; has no knowledge of balances. |
| **Ledger** | Accounts, holds, immutable double-entry postings | Single source of truth for money. Must be simple and correct. |
| **Fraud** | Rules, risk decisions | Changes often, can be slow; must not block money core. |
| **Bank Gateway Simulator** | External bank behaviour | Models unreliable third parties. |
| **Statement** | Read-optimised history | CQRS: reads never load the ledger. |
| **Notification** | Email templates, delivery, retries | Uses RabbitMQ for task-queue semantics. |

### 3.3 Communication rules
- **Client → system:** synchronous REST through the gateway.
- **Service ↔ service:** asynchronous via Kafka only. No service-to-service REST calls in the payment path.
- **Notification delivery:** Kafka in, **RabbitMQ** for the email task queue.
- **Database per service.** No shared tables, no cross-service joins.

---

## 4. Messaging Strategy: Kafka vs RabbitMQ

### 4.1 Division of responsibility

| Concern | Kafka | RabbitMQ |
|---|---|---|
| Domain events and commands | ✅ Everything | ❌ |
| Needs replay / audit / many consumers | ✅ | ❌ |
| Per-key ordering (per account) | ✅ Partitions | ❌ |
| "Do this job once, retry with delay, park failures" | Awkward | ✅ Native (TTL + DLX) |
| Sending a customer email | ❌ | ✅ |

**Rule:** Kafka is the system's memory (facts that happened). RabbitMQ is the system's to-do list (work to perform).

### 4.2 Kafka topics

| Topic | Key | Partitions | Producer | Consumers |
|---|---|---|---|---|
| `payment.events` | `paymentId` | 6 | Payment | Fraud, Statement, Notification |
| `fraud.events` | `paymentId` | 6 | Fraud | Payment |
| `ledger.commands` | **source `accountId`** | 6 | Payment | Ledger |
| `ledger.events` | `accountId` | 6 | Ledger | Payment, Statement |
| `gateway.commands` | `paymentId` | 3 | Payment | Gateway Simulator |
| `gateway.events` | `paymentId` | 3 | Gateway Simulator | Payment |
| `notification.requests` | `customerId` | 3 | Payment (via outbox) | Notification |
| `<topic>.DLT` | same as original | same | Error handlers | Ops tooling |

**Why `ledger.commands` is keyed by account:** all commands for one account land in one partition, so they are processed **sequentially by one consumer thread**. This is the *single-writer principle* and removes most contention. The database still enforces correctness with optimistic locking and constraints (defence in depth).

### 4.3 Producer / consumer configuration (production defaults)

**Producer**
```
acks=all
enable.idempotence=true
max.in.flight.requests.per.connection=5
retries=Integer.MAX_VALUE
delivery.timeout.ms=120000
compression.type=lz4
linger.ms=10
```

**Consumer**
```
enable.auto.commit=false          # ack manually, after the DB transaction commits
isolation.level=read_committed
auto.offset.reset=earliest
max.poll.records=50
```

**Cluster (prod values; local compose uses 1 broker):** `replication.factor=3`, `min.insync.replicas=2`, `unclean.leader.election.enable=false`.

**Error handling:** `DefaultErrorHandler` with `ExponentialBackOff` (e.g. 1s → 2s → 4s, max 5 attempts), then `DeadLetterPublishingRecoverer` to `<topic>.DLT`. Non-retryable exceptions (validation, deserialization, unknown event version) go straight to the DLT.

**Serialization:** JSON with an explicit `schemaVersion` in phase 1. Phase 2 option: Avro + Schema Registry with backward-compatible evolution.

### 4.4 RabbitMQ topology (Notification Service only)

```
 Notification Service                   RabbitMQ
 ┌────────────────────┐   publish    ┌───────────────────┐
 │ Kafka consumer     │─────────────▶│ exchange           │
 │ (notification.     │  (confirm)   │ notification.direct│
 │  requests)         │              └─────────┬─────────┘
 └────────────────────┘                        │ rk=email
                                               ▼
                                     ┌───────────────────┐   fail   ┌──────────────────────┐
                                     │ queue             │─────────▶│ notification.retry.  │
                                     │ notification.email│          │ exchange             │
                                     └─────────┬─────────┘          └─────┬────────────────┘
                                               │                          ▼
                                     Email worker (manual ack)     retry.5s ─▶ retry.30s ─▶ retry.5m
                                               │                    (per-queue TTL, dead-letter
                                               ▼                     back to notification.email)
                                             SMTP                           │ after max attempts
                                                                            ▼
                                                                 notification.email.dlq (parking lot)
```

- **Publisher confirms** enabled; persistent messages; durable queues.
- **Manual acks**, `prefetch=10`, concurrency tuned to SMTP rate limits.
- **Retry by TTL queues:** failed message goes to a retry queue with a TTL, then dead-letters back to the main queue. Attempt count is carried in a header (`x-attempt`).
- **After max attempts → `notification.email.dlq`.** Visible in the management UI, replayable by an admin endpoint.
- **Idempotency:** `notificationId` is checked against `notification_log` before sending, so a redelivered task never sends a second email.
- Local SMTP: **MailHog**.

---

## 5. Event and Message Contracts

### 5.1 Common envelope (all Kafka events/commands)

```json
{
  "eventId": "0f8fad5b-d9cb-469f-a165-70867728950e",
  "eventType": "PaymentInitiated",
  "schemaVersion": 1,
  "aggregateId": "pay_01HF...",
  "occurredAt": "2026-10-29T10:15:30.123Z",
  "correlationId": "c0d1...",
  "causationId": "previous-event-id",
  "producer": "payment-service",
  "payload": { }
}
```

Kafka headers: `traceparent` (W3C trace context), `eventType`, `schemaVersion`.

- `eventId` is the **idempotency key for consumers** (inbox table).
- `correlationId` stays constant for the whole business flow; `causationId` points to the event that caused this one.

### 5.2 Payment events (`payment.events`)

**PaymentInitiated**
```json
{
  "paymentId": "pay_01HF...",
  "type": "UPI_PAY",
  "sourceAccountId": "acc_123",
  "destination": { "kind": "EXTERNAL_VPA", "vpa": "cafe@somebank" },
  "amountMinor": 50000,
  "currency": "INR",
  "remark": "Coffee"
}
```
`destination.kind` is `INTERNAL_ACCOUNT` (with `accountId`) or `EXTERNAL_VPA`.

**PaymentCompleted**
```json
{ "paymentId": "pay_01HF...", "sourceAccountId": "acc_123", "amountMinor": 50000,
  "currency": "INR", "bankReference": "BNK778812", "completedAt": "2026-10-29T10:15:34Z" }
```

**PaymentFailed**
```json
{ "paymentId": "pay_01HF...", "sourceAccountId": "acc_123", "amountMinor": 50000,
  "reasonCode": "INSUFFICIENT_FUNDS", "reasonMessage": "Available balance too low" }
```
`reasonCode` ∈ `FRAUD_REJECTED`, `INSUFFICIENT_FUNDS`, `ACCOUNT_BLOCKED`, `BANK_DECLINED`, `BANK_TIMEOUT_RECONCILED_FAILED`, `INTERNAL_ERROR`.

**PaymentStatusChanged** (every state transition, feeds audit and statement)
```json
{ "paymentId": "pay_01HF...", "from": "FUNDS_RESERVING", "to": "PROCESSING_WITH_BANK" }
```

### 5.3 Fraud events (`fraud.events`)

**FraudDecisionMade**
```json
{ "paymentId": "pay_01HF...", "decision": "APPROVED",
  "riskScore": 12, "rulesTriggered": [] }
```
`decision` ∈ `APPROVED`, `REJECTED`, `REVIEW` (phase 1 treats `REVIEW` as reject).

### 5.4 Ledger commands (`ledger.commands`)

**ReserveFunds**
```json
{ "paymentId": "pay_01HF...", "accountId": "acc_123", "amountMinor": 50000,
  "holdExpiresAt": "2026-10-29T10:25:30Z" }
```

**CaptureHold**
```json
{ "paymentId": "pay_01HF...", "holdId": "hold_9", "counterparty": { "kind": "EXTERNAL_SETTLEMENT" } }
```
For wallet→wallet, `counterparty` is `{ "kind": "INTERNAL_ACCOUNT", "accountId": "acc_456" }`.

**ReleaseHold**
```json
{ "paymentId": "pay_01HF...", "holdId": "hold_9", "reason": "BANK_DECLINED" }
```

### 5.5 Ledger events (`ledger.events`)

| Event | Payload highlights |
|---|---|
| `FundsReserved` | `paymentId, holdId, accountId, amountMinor, availableBalanceAfterMinor` |
| `FundsReservationFailed` | `paymentId, accountId, reasonCode` (`INSUFFICIENT_FUNDS`, `ACCOUNT_BLOCKED`, `ACCOUNT_NOT_FOUND`) |
| `HoldCaptured` | `paymentId, transactionId, entries[{accountId, direction, amountMinor, balanceAfterMinor}]` |
| `HoldReleased` | `paymentId, holdId, accountId, amountMinor` |
| `AccountCredited` | `accountId, amountMinor, transactionId, paymentId` (top-ups and internal transfers) |
| `HoldExpired` | `holdId, paymentId` (emitted by the expiry sweeper) |

### 5.6 Gateway messages

**`gateway.commands` → TransferRequested**
```json
{ "paymentId": "pay_01HF...", "gatewayRequestId": "gw_req_1",
  "toVpa": "cafe@somebank", "amountMinor": 50000, "currency": "INR" }
```

**`gateway.events`**
```json
{ "eventType": "TransferSucceeded", "payload": { "paymentId": "...", "gatewayRequestId": "gw_req_1", "bankReference": "BNK778812" } }
{ "eventType": "TransferDeclined",  "payload": { "paymentId": "...", "gatewayRequestId": "gw_req_1", "declineCode": "VPA_NOT_FOUND" } }
```
No message at all (a timeout) is a valid outcome and handled by the reconciliation job.

### 5.7 Notification request (`notification.requests`)

```json
{
  "notificationId": "ntf_01HF...",
  "customerId": "cust_77",
  "channel": "EMAIL",
  "template": "PAYMENT_SUCCESS",
  "to": "riya@example.com",
  "params": { "amount": "₹500.00", "payee": "cafe@somebank", "paymentId": "pay_01HF...", "date": "2026-10-29" }
}
```
`notificationId` is derived deterministically from `paymentId + template` so duplicates collapse.

---

## 6. REST API Contracts

All write endpoints require `Authorization: Bearer <JWT>` and `Idempotency-Key: <uuid>` (payments, top-ups). Errors use **RFC 7807 Problem Details**.

### 6.1 Payment Service

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/v1/payments` | Initiate a payment → `202 Accepted` |
| `GET` | `/api/v1/payments/{paymentId}` | Current status and timeline |
| `GET` | `/api/v1/payments?accountId=&status=&page=&size=` | List payments |

**Request**
```json
POST /api/v1/payments
Idempotency-Key: 6b1c...
{
  "type": "UPI_PAY",
  "sourceAccountId": "acc_123",
  "destination": { "kind": "EXTERNAL_VPA", "vpa": "cafe@somebank" },
  "amountMinor": 50000,
  "currency": "INR",
  "remark": "Coffee"
}
```

**Response `202`**
```json
{ "paymentId": "pay_01HF...", "status": "INITIATED", "createdAt": "2026-10-29T10:15:30Z" }
```

**Behaviour for duplicates:** same key + same body → returns the original response. Same key + **different** body → `422` (`IDEMPOTENCY_KEY_REUSED_WITH_DIFFERENT_PAYLOAD`). Same key while the first is still in flight → `409`.

### 6.2 Ledger / Account Service

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/v1/accounts` | Create account (generates VPA) |
| `GET` | `/api/v1/accounts/{accountId}` | Account details |
| `GET` | `/api/v1/accounts/{accountId}/balance` | `available`, `held`, `ledger` balances |
| `GET` | `/api/v1/accounts/resolve?vpa=` | Resolve a VPA (Redis cache-aside) |
| `POST` | `/api/v1/accounts/{accountId}/topups` | Start a top-up from external bank |

### 6.3 Statement Service

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/v1/accounts/{accountId}/statement?from=&to=&cursor=&limit=` | Keyset-paginated entries |
| `GET` | `/api/v1/accounts/{accountId}/statement/summary?month=2026-10` | Monthly totals |

### 6.4 Admin / Ops (internal only)

| Method | Path | Description |
|---|---|---|
| `POST` | `/admin/dlt/{topic}/replay` | Replay selected DLT records |
| `POST` | `/admin/notifications/dlq/replay` | Replay RabbitMQ DLQ |
| `POST` | `/admin/payments/{id}/reconcile` | Force reconciliation for a stuck payment |

---

## 7. Data Model (PostgreSQL, one database per service)

Conventions: IDs are prefixed ULIDs stored as `VARCHAR(40)`; money is `BIGINT` paise; every table has `created_at`; mutable tables have `updated_at` and `version` (optimistic locking).

### 7.1 payment_db

```sql
CREATE TABLE payments (
  id                 VARCHAR(40) PRIMARY KEY,
  type               VARCHAR(20)  NOT NULL,   -- WALLET_TRANSFER, UPI_PAY, TOPUP
  source_account_id  VARCHAR(40)  NOT NULL,
  dest_kind          VARCHAR(20)  NOT NULL,   -- INTERNAL_ACCOUNT, EXTERNAL_VPA
  dest_account_id    VARCHAR(40),
  dest_vpa           VARCHAR(100),
  amount_minor       BIGINT       NOT NULL CHECK (amount_minor > 0),
  currency           CHAR(3)      NOT NULL DEFAULT 'INR',
  status             VARCHAR(30)  NOT NULL,
  failure_code       VARCHAR(50),
  hold_id            VARCHAR(40),
  gateway_request_id VARCHAR(40),
  bank_reference     VARCHAR(60),
  remark             VARCHAR(140),
  correlation_id     VARCHAR(40)  NOT NULL,
  created_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
  updated_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
  next_check_at      TIMESTAMPTZ,              -- used by timeout/reconciliation sweeper
  version            BIGINT       NOT NULL DEFAULT 0
);
CREATE INDEX idx_payments_source ON payments (source_account_id, created_at DESC);
CREATE INDEX idx_payments_stuck  ON payments (status, next_check_at)
  WHERE status IN ('PROCESSING_WITH_BANK','UNKNOWN');

CREATE TABLE payment_state_history (
  id          BIGSERIAL PRIMARY KEY,
  payment_id  VARCHAR(40) NOT NULL REFERENCES payments(id),
  from_status VARCHAR(30),
  to_status   VARCHAR(30) NOT NULL,
  reason      VARCHAR(200),
  event_id    VARCHAR(40),
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE idempotency_keys (
  idem_key       VARCHAR(80) NOT NULL,
  client_id      VARCHAR(40) NOT NULL,
  request_hash   CHAR(64)    NOT NULL,        -- SHA-256 of canonical body
  payment_id     VARCHAR(40),
  response_code  INT,
  response_body  JSONB,
  state          VARCHAR(15) NOT NULL,        -- IN_PROGRESS, COMPLETED
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at     TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (client_id, idem_key)
);

CREATE TABLE outbox_events (
  id             VARCHAR(40) PRIMARY KEY,
  aggregate_id   VARCHAR(40) NOT NULL,
  topic          VARCHAR(80) NOT NULL,
  message_key    VARCHAR(80) NOT NULL,
  event_type     VARCHAR(60) NOT NULL,
  payload        JSONB       NOT NULL,
  headers        JSONB,
  status         VARCHAR(10) NOT NULL DEFAULT 'PENDING',  -- PENDING, SENT
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  sent_at        TIMESTAMPTZ
);
CREATE INDEX idx_outbox_pending ON outbox_events (created_at) WHERE status = 'PENDING';

CREATE TABLE processed_events (      -- inbox / consumer dedupe
  consumer_name VARCHAR(60) NOT NULL,
  event_id      VARCHAR(40) NOT NULL,
  processed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer_name, event_id)
);
```

### 7.2 ledger_db

```sql
CREATE TABLE accounts (
  id              VARCHAR(40) PRIMARY KEY,
  customer_id     VARCHAR(40) NOT NULL,
  vpa             VARCHAR(100) NOT NULL UNIQUE,
  type            VARCHAR(20) NOT NULL,        -- CUSTOMER, SETTLEMENT, FEE
  status          VARCHAR(15) NOT NULL,        -- ACTIVE, BLOCKED, CLOSED
  currency        CHAR(3)     NOT NULL DEFAULT 'INR',
  balance_minor   BIGINT      NOT NULL DEFAULT 0,   -- posted balance
  held_minor      BIGINT      NOT NULL DEFAULT 0,   -- sum of active holds
  version         BIGINT      NOT NULL DEFAULT 0,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_held_nonneg   CHECK (held_minor >= 0),
  CONSTRAINT chk_no_overdraft  CHECK (type <> 'CUSTOMER' OR balance_minor - held_minor >= 0)
);

CREATE TABLE holds (
  id           VARCHAR(40) PRIMARY KEY,
  payment_id   VARCHAR(40) NOT NULL UNIQUE,      -- one hold per payment (idempotency)
  account_id   VARCHAR(40) NOT NULL REFERENCES accounts(id),
  amount_minor BIGINT      NOT NULL CHECK (amount_minor > 0),
  status       VARCHAR(10) NOT NULL,             -- HELD, CAPTURED, RELEASED, EXPIRED
  expires_at   TIMESTAMPTZ NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_holds_expiry ON holds (expires_at) WHERE status = 'HELD';

CREATE TABLE ledger_transactions (
  id          VARCHAR(40) PRIMARY KEY,
  payment_id  VARCHAR(40) NOT NULL UNIQUE,       -- a payment posts at most once
  kind        VARCHAR(20) NOT NULL,              -- TRANSFER, TOPUP, REVERSAL
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE ledger_entries (                     -- IMMUTABLE: insert only
  id                   BIGSERIAL PRIMARY KEY,
  transaction_id       VARCHAR(40) NOT NULL REFERENCES ledger_transactions(id),
  account_id           VARCHAR(40) NOT NULL REFERENCES accounts(id),
  direction            CHAR(1)     NOT NULL CHECK (direction IN ('D','C')),
  amount_minor         BIGINT      NOT NULL CHECK (amount_minor > 0),
  balance_after_minor  BIGINT      NOT NULL,
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_entries_account ON ledger_entries (account_id, id DESC);
-- Invariant (asserted in code AND by a nightly job):
-- for every transaction_id: SUM(D) = SUM(C)

CREATE TABLE outbox_events (...);       -- same shape as payment_db
CREATE TABLE processed_events (...);    -- same shape as payment_db
```
Protect immutability with a trigger that rejects `UPDATE`/`DELETE` on `ledger_entries`, and a restricted DB role for the application.

### 7.3 fraud_db

```sql
CREATE TABLE fraud_decisions (
  payment_id   VARCHAR(40) PRIMARY KEY,
  account_id   VARCHAR(40) NOT NULL,
  decision     VARCHAR(10) NOT NULL,
  risk_score   INT         NOT NULL,
  rules        JSONB       NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE fraud_rules (
  id VARCHAR(40) PRIMARY KEY, name VARCHAR(60) NOT NULL, enabled BOOLEAN NOT NULL,
  params JSONB NOT NULL, weight INT NOT NULL
);
-- outbox_events, processed_events as above
```
Velocity counters (payments per minute/hour, spend per day, distinct new payees) live in **Redis** sorted sets / counters with TTLs.

### 7.4 gateway_db

```sql
CREATE TABLE gateway_transactions (
  gateway_request_id VARCHAR(40) PRIMARY KEY,
  payment_id         VARCHAR(40) NOT NULL UNIQUE,
  to_vpa             VARCHAR(100) NOT NULL,
  amount_minor       BIGINT NOT NULL,
  status             VARCHAR(15) NOT NULL,        -- SUCCESS, DECLINED, PENDING
  bank_reference     VARCHAR(60),
  scenario           VARCHAR(30) NOT NULL,        -- see §10
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
```
Also exposes `GET /gateway/transactions/{gatewayRequestId}` for the **status-enquiry** API used by reconciliation.

### 7.5 statement_db (read model)

```sql
CREATE TABLE statement_entries (
  id                 BIGSERIAL PRIMARY KEY,
  account_id         VARCHAR(40) NOT NULL,
  transaction_id     VARCHAR(40) NOT NULL,
  payment_id         VARCHAR(40),
  direction          CHAR(1)     NOT NULL,
  amount_minor       BIGINT      NOT NULL,
  balance_after_minor BIGINT     NOT NULL,
  counterparty       VARCHAR(100),
  description        VARCHAR(200),
  posted_at          TIMESTAMPTZ NOT NULL,
  UNIQUE (transaction_id, account_id)             -- natural idempotency for the projector
);
CREATE INDEX idx_stmt_account_time ON statement_entries (account_id, posted_at DESC, id DESC);

CREATE TABLE daily_summary (
  account_id VARCHAR(40), day DATE,
  total_debit_minor BIGINT, total_credit_minor BIGINT, closing_balance_minor BIGINT,
  PRIMARY KEY (account_id, day)
);
```

### 7.6 notification_db

```sql
CREATE TABLE notification_log (
  notification_id VARCHAR(40) PRIMARY KEY,
  customer_id     VARCHAR(40) NOT NULL,
  template        VARCHAR(40) NOT NULL,
  recipient       VARCHAR(200) NOT NULL,
  status          VARCHAR(12) NOT NULL,          -- QUEUED, SENT, FAILED
  attempts        INT         NOT NULL DEFAULT 0,
  last_error      VARCHAR(500),
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  sent_at         TIMESTAMPTZ
);
CREATE TABLE email_templates (
  code VARCHAR(40) PRIMARY KEY, subject VARCHAR(200) NOT NULL, body_html TEXT NOT NULL, version INT NOT NULL
);
```

---

## 8. Payment Saga

### 8.1 State machine

```
                 ┌──────────────┐
                 │  INITIATED   │
                 └──────┬───────┘
                        │ PaymentInitiated published
                        ▼
                 ┌──────────────┐  REJECTED   ┌────────┐
                 │FRAUD_CHECKING│────────────▶│ FAILED │◀────────────────────┐
                 └──────┬───────┘             └────────┘                     │
                        │ APPROVED                 ▲                         │
                        ▼                          │ ReservationFailed       │
                 ┌──────────────┐──────────────────┘                         │
                 │FUNDS_RESERVING│                                           │
                 └──────┬───────┘                                            │
                        │ FundsReserved                                      │
                        ▼                                                    │
                 ┌──────────────────┐  TransferDeclined   ┌───────────┐      │
                 │PROCESSING_WITH_  │────────────────────▶│ RELEASING │──────┘
                 │      BANK        │                     └───────────┘ HoldReleased
                 └───┬──────────┬───┘                           ▲
        TransferSucceeded│      │ no answer before deadline      │ reconcile says FAILED
                         ▼      ▼                                │
                 ┌──────────┐  ┌─────────┐   reconcile        ───┘
                 │CAPTURING │  │ UNKNOWN │───────────────────▶ (SUCCESS → CAPTURING)
                 └────┬─────┘  └─────────┘
                      │ HoldCaptured
                      ▼
                 ┌──────────┐
                 │COMPLETED │
                 └──────────┘
```

Terminal states: `COMPLETED`, `FAILED`. A completed payment is never mutated; a refund is a **new payment** (`type=REVERSAL`) referencing the original.

### 8.2 Transition rules
- Transitions are validated by a **state-machine class** (illegal transitions throw and are logged).
- Every transition: update `payments` (optimistic lock) + insert `payment_state_history` + insert `outbox_events` **in one DB transaction**.
- Events that arrive for a payment already in a later state (late/duplicate) are **ignored safely** and logged.

### 8.3 Happy path (UPI pay)

```
Client      Payment        Fraud        Ledger       Gateway     Statement   Notification
  │ POST /payments │          │            │            │            │            │
  │───────────────▶│ INITIATED + outbox     │            │            │            │
  │◀── 202 ────────│          │            │            │            │            │
  │                │─PaymentInitiated─▶     │            │            │            │
  │                │◀─FraudDecision(APPROVED)            │            │            │
  │                │─ReserveFunds──────────▶│            │            │            │
  │                │◀─FundsReserved─────────│            │            │            │
  │                │─TransferRequested──────────────────▶│            │            │
  │                │◀─TransferSucceeded──────────────────│            │            │
  │                │─CapturePayment────────▶│            │            │            │
  │                │◀─HoldCaptured──────────│──HoldCaptured──────────▶│            │
  │                │─PaymentCompleted────────────────────────────────▶│──email task│
```

### 8.4 Wallet → Wallet
Same flow, but `PROCESSING_WITH_BANK` is skipped: after `FundsReserved`, Payment sends `CaptureHold` with the internal destination account and Ledger posts debit source / credit destination atomically in one local transaction.

### 8.5 Top-up
Payment (type `TOPUP`) → Gateway simulator "collects" from the user's bank → on success Ledger posts credit user / debit settlement account.

---

## 9. Design Principles and Patterns Used

| Pattern / Principle | Where it is applied |
|---|---|
| **Database per service** | Every service owns its schema. |
| **Saga (orchestration)** | Payment Service drives the multi-step flow with compensations. |
| **Transactional Outbox** | State change + event written in one transaction; a relay publishes to Kafka. |
| **Inbox / idempotent consumer** | `processed_events` table; plus natural unique constraints. |
| **CQRS + event-sourced projections** | Statement Service builds its own read model from ledger events. |
| **Double-entry bookkeeping** | Every money movement posts equal debits and credits; entries are immutable. |
| **Hold / capture (two-phase money)** | Reserve first, capture only after the bank confirms. |
| **Single-writer via partition key** | Per-account serialisation in `ledger.commands`. |
| **Optimistic locking** | `version` columns; retry on conflict. |
| **State machine** | Explicit payment lifecycle with guarded transitions. |
| **Strategy** | Fraud rules (`FraudRule` interface), gateway scenarios, notification channels. |
| **Chain of Responsibility** | Fraud rule pipeline accumulates risk score. |
| **Factory** | Notification message and template creation. |
| **Circuit Breaker, Retry, Bulkhead, Timeout (Resilience4j)** | SMTP calls, status-enquiry calls to the gateway. |
| **Cache-aside** | VPA → account resolution in Redis. |
| **Token bucket rate limiting** | Gateway-level, per client, in Redis. |
| **Hexagonal architecture** | `domain` / `application` / `adapter` packages, domain has no framework imports. |
| **Anti-corruption layer** | Gateway client adapter translates bank-specific codes to domain codes. |
| **Dead Letter + Parking Lot** | Kafka DLT and RabbitMQ DLQ with replay tooling. |
| **12-factor config** | Environment-based configuration, secrets not in code. |

SOLID is enforced by structure: small ports/interfaces in `application`, implementations in `adapter`.

---

## 10. Idempotency, Concurrency and Caching Strategy

### 10.1 Idempotency at every layer

| Layer | Mechanism |
|---|---|
| **Client → API** | `Idempotency-Key` + request hash in `idempotency_keys` (`INSERT ... ON CONFLICT DO NOTHING`; state `IN_PROGRESS` → `COMPLETED`). Redis holds a short-TTL fast path; PostgreSQL is the source of truth. |
| **Kafka producer** | `enable.idempotence=true`. |
| **Kafka consumer** | `processed_events (consumer_name, event_id)` inserted in the **same transaction** as the business change. Duplicate → skip. |
| **Ledger** | `holds.payment_id UNIQUE`, `ledger_transactions.payment_id UNIQUE`: a second reserve or capture for the same payment is a no-op returning the original result. |
| **Gateway** | `gatewayRequestId` is passed to the bank; the simulator returns the same result for the same id. |
| **Notification** | `notification_log.notification_id` PK checked before sending. |

### 10.2 Locking

- **Primary protection:** partition key = `accountId` (single writer per account).
- **Second line:** optimistic locking on `accounts.version`; on `OptimisticLockException` retry up to N times with jitter.
- **Third line:** DB `CHECK (balance_minor - held_minor >= 0)` makes overdraft *impossible* even if a bug slips through.
- For **internal transfers touching two accounts**, always lock/update in **ascending `accountId` order** to avoid deadlocks (`SELECT ... FOR UPDATE` in that order if using pessimistic locking).
- **Redis distributed lock** (with fencing token) is used only for singleton jobs (reconciliation sweeper, hold-expiry sweeper), never to guard balances. ShedLock is a good fit.

### 10.3 Caching rules (Redis)

| Data | Strategy | TTL | Note |
|---|---|---|---|
| VPA → account | Cache-aside | 10 min | Evict on account status change. |
| Idempotency fast-path | Write-through | 24 h | DB remains authoritative. |
| Rate-limit buckets | Lua token bucket | Sliding | Atomic via Lua script. |
| Fraud velocity counters | ZSET sliding window | 24 h | Rebuildable from DB. |
| **Balances** | **Never cached for authorisation** | n/a | Display-only reads may be cached for a few seconds. |

Defences: null-caching for unknown VPAs (short TTL), TTL jitter to avoid stampedes, single-flight loading for hot keys.

---

## 11. Edge Cases and Failure Handling

| # | Scenario | Handling |
|---|---|---|
| 1 | Client retries `POST /payments` after timeout | Idempotency key returns original result. |
| 2 | Same key, different body | `422`, no side effects. |
| 3 | Two concurrent payments drain the same balance | Partition ordering + optimistic lock + CHECK constraint: one fails with `INSUFFICIENT_FUNDS`. |
| 4 | Kafka message delivered twice | Inbox table skips the duplicate. |
| 5 | Consumer crashes after DB commit, before offset commit | Redelivery hits the inbox, skipped. |
| 6 | Service crashes after DB commit, before Kafka publish | Outbox relay publishes on restart (at-least-once). |
| 7 | Out-of-order events for a payment | State machine rejects illegal transitions; logs and ignores. |
| 8 | Fraud service is down / slow | Payment waits in `FRAUD_CHECKING` with a timeout; after the deadline, apply policy (phase 1: fail the payment safely). |
| 9 | Fraud rejects | `FAILED(FRAUD_REJECTED)`, no hold ever created. |
| 10 | Insufficient funds / blocked account | `FundsReservationFailed` → `FAILED`. |
| 11 | Bank declines | `RELEASING` → hold released → `FAILED`. |
| 12 | **Bank times out (outcome unknown)** | `UNKNOWN`. **Never release the hold blindly.** The reconciliation job calls the gateway's status-enquiry API with the same `gatewayRequestId` and then captures or releases. |
| 13 | Bank answers twice (success twice) | Second event ignored (payment already past that state; inbox). |
| 14 | Bank answers **success after we already released** | Detected as an anomaly: raise an alert, create a `MANUAL_REVIEW` record, auto-create a reversal path. This is why `UNKNOWN` must reconcile before releasing. |
| 15 | Hold never captured or released (crash mid-saga) | `HoldExpirySweeper` finds expired holds, asks Payment for status, releases only if the payment is terminal-failed. |
| 16 | Poison message (bad JSON) | Non-retryable → straight to DLT with error headers. |
| 17 | Unknown `schemaVersion` | DLT, alert; producers must stay backward compatible. |
| 18 | Consumer lag spike | Alert on lag; scale consumers up to partition count. |
| 19 | Self-transfer (source = destination) | Rejected at validation (`400`). |
| 20 | Amount ≤ 0, > per-txn limit, > daily limit | Validated at API, enforced again in Fraud. |
| 21 | Destination account closed / VPA unknown | Fails fast with a clear reason code. |
| 22 | Email provider down | RabbitMQ retry queues with backoff, then DLQ; payment is unaffected. |
| 23 | Invalid / missing customer email | `FAILED` notification, no retry loop (non-retryable). |
| 24 | Duplicate email task | `notification_log` blocks the second send. |
| 25 | Clock skew / expired holds | Use DB time (`now()`) for authoritative expiry decisions. |
| 26 | Broker restart / rebalance | Cooperative rebalancing, idempotent processing, no data loss with `acks=all`. |
| 27 | Replay of a topic to rebuild Statement | Projector is idempotent (`UNIQUE(transaction_id, account_id)`). |
| 28 | Ledger invariant violation | Nightly job checks `SUM(D)=SUM(C)` per transaction and `balance = SUM(entries)` per account; pages on any mismatch. |

### Bank Gateway Simulator scenarios
Selectable per request (via VPA naming convention or admin config) so we can test each edge case deterministically:

`SUCCESS`, `DECLINE_INSUFFICIENT_AT_BANK`, `DECLINE_VPA_NOT_FOUND`, `SLOW_SUCCESS` (delayed 10 s), `TIMEOUT_NEVER_RESPOND`, `TIMEOUT_THEN_SUCCESS`, `DUPLICATE_SUCCESS` (two callbacks), `FLAKY` (random mix), `SUCCESS_THEN_STATUS_ENQUIRY_ONLY`.

Example: VPA `timeout@somebank` triggers `TIMEOUT_NEVER_RESPOND`.

---

## 12. Fraud Rules (phase 1)

| Rule | Logic | Weight |
|---|---|---|
| Amount threshold | Single txn above ₹50,000 | 40 |
| Velocity (short) | > 5 payments in 1 minute | 50 |
| Velocity (daily) | Daily spend above configured cap | 60 |
| New payee + high amount | First payment to a VPA and above ₹10,000 | 30 |
| Odd hours | 1 AM–5 AM and above ₹5,000 | 15 |
| Blocked list | Payee VPA on a deny list | 100 |

Decision: score ≥ 80 → `REJECTED`; 40–79 → `REVIEW` (treated as reject in phase 1); below 40 → `APPROVED`. Rules implement a common `FraudRule` interface and are loaded from `fraud_rules`, so thresholds change without redeploying.

---

## 13. Cross-Cutting Concerns

### 13.1 Observability
- **Logging:** structured JSON; MDC fields `correlationId`, `paymentId`, `accountId`; no PII or full VPAs in logs (mask).
- **Tracing:** Micrometer Tracing + OpenTelemetry; trace context propagated in Kafka and RabbitMQ headers; Jaeger or Tempo locally.
- **Metrics (Prometheus + Grafana):** payments by status, end-to-end latency, saga stuck count, outbox backlog, consumer lag, DLT message count, RabbitMQ queue depth, email success rate, cache hit ratio.
- **Health:** Actuator liveness/readiness, including Kafka/DB/Redis/RabbitMQ indicators.
- **Alerts:** stuck payments in `UNKNOWN` > 5 min, outbox age > 30 s, any DLT growth, ledger invariant failure.

### 13.2 Security
- JWT validated at the gateway and re-validated in services (resource server).
- A user may only act on their own accounts (ownership check in Payment and Ledger).
- Secrets via environment/Docker secrets, never committed.
- Input validation (Bean Validation) and strict request size limits.
- Audit trail: `payment_state_history` plus immutable ledger entries.
- Kafka and RabbitMQ credentials per service; least privilege ACLs (production note).

### 13.3 Configuration and deployment
- `application.yml` with profiles `local`, `test`, `prod`; env-var overrides.
- Flyway migrations per service.
- Docker multi-stage builds, non-root user, health checks.
- `docker-compose.yml` services: Kafka (KRaft), Kafka UI, RabbitMQ (+ management), PostgreSQL ×6 (or one instance with six databases locally), Redis, MailHog, Prometheus, Grafana, Jaeger.
- Graceful shutdown enabled so in-flight messages finish before exit.

---

## 14. Project Structure

Multi-module Maven (or Gradle) repository:

```
payflow/
├── pom.xml
├── common/
│   ├── common-events/          # envelope, event/command DTOs, topic constants
│   ├── common-outbox/          # outbox entity, relay, inbox helper (starter)
│   ├── common-web/             # problem-details handler, correlation filter
│   └── common-test/            # Testcontainers base classes
├── api-gateway/
├── payment-service/
├── ledger-service/
├── fraud-service/
├── bank-gateway-simulator/
├── statement-service/
├── notification-service/
├── deploy/
│   ├── docker-compose.yml
│   ├── prometheus/ grafana/
│   └── kafka-topics.sh
└── docs/
    └── payflow-design.md
```

Per-service package layout (hexagonal):

```
com.payflow.payment
├── domain/        # Payment, PaymentStatus, PaymentStateMachine, domain events, exceptions
├── application/   # use cases (InitiatePayment, HandleFraudDecision...), ports (interfaces)
├── adapter/
│   ├── in/web/    # controllers, request/response DTOs
│   ├── in/kafka/  # listeners
│   └── out/       # JPA repositories, outbox publisher, gateway client
└── config/
```

---

## 15. Testing Strategy

| Level | What | Tools |
|---|---|---|
| Unit | State machine, fraud rules, money math, idempotency logic | JUnit 5, AssertJ, Mockito |
| Slice | Controllers, repositories | `@WebMvcTest`, `@DataJpaTest` |
| Integration | Service + real Postgres/Kafka/Redis/RabbitMQ | Testcontainers |
| Contract | Event schemas between producers and consumers | Spring Cloud Contract or JSON-schema tests |
| Saga end-to-end | Each simulator scenario from §11 produces the right final state | Testcontainers compose, Awaitility |
| Concurrency | 100 parallel debits on one account; no overdraft, correct final balance | Executors + assertions |
| Chaos-style | Kill a consumer mid-processing, restart the broker, drop gateway responses | Scripted tests |
| Performance | Sustained load and consumer-lag behaviour | Gatling or k6 |
| Invariants | Ledger sum checks after every test run | Reusable assertion helper |

---

## 16. Build Plan (phased, each phase runnable)

| Phase | Deliverable | Concepts practised |
|---|---|---|
| **0** | Repo skeleton, `common` modules, docker-compose (Kafka, Postgres, Redis, RabbitMQ, MailHog), Flyway, topic creation | Infra and conventions |
| **1** | **Ledger Service**: accounts, holds, double-entry, REST, tests | Money correctness, locking, constraints |
| **2** | **Payment Service**: REST + idempotency + state machine + outbox relay | Idempotency, outbox, saga state |
| **3** | Kafka wiring Payment ↔ Ledger (reserve / capture / release) with inbox, retry, DLT | Consumers, partitions, error handling |
| **4** | **Bank Gateway Simulator** + UPI flow + timeout handling and reconciliation | Unreliable externals, `UNKNOWN` state |
| **5** | **Fraud Service** with Redis velocity rules | Strategy, Redis data structures |
| **6** | **Statement Service** (CQRS projection, keyset pagination, replay) | Read models, idempotent projections |
| **7** | **Notification Service**: Kafka → RabbitMQ → email worker, retry/DLQ, templates | RabbitMQ topology, task queues |
| **8** | API Gateway, JWT, rate limiting | Edge concerns |
| **9** | Observability stack, dashboards, alerts | Metrics, tracing, lag monitoring |
| **10** | Chaos/performance tests, ledger invariant job, hardening, README | Production readiness |

**Suggested first coding session:** Phase 0 + Phase 1 (infra and Ledger Service), since every other service depends on it.

---

## 17. Open Decisions (to confirm before coding)

1. **Build tool:** Maven or Gradle?
2. **Outbox relay:** start with a polling publisher (simple, in-app) and optionally move to Debezium CDC later: agreed?
3. **Event format:** JSON in phase 1, Avro + Schema Registry later?
4. **Local Postgres:** one container with six databases, or six containers?
5. **Fraud `REVIEW` outcome:** treat as reject in phase 1?

---

## 18. Glossary

| Term | Meaning |
|---|---|
| **VPA** | Virtual Payment Address (UPI ID), e.g. `name@bank`. |
| **Hold** | Temporarily reserved portion of a balance, not yet moved. |
| **Capture** | Converting a hold into real ledger postings. |
| **Outbox** | Table written in the same DB transaction as business data, then relayed to the broker. |
| **Inbox** | Table recording processed message IDs to make consumers idempotent. |
| **DLT / DLQ** | Dead-letter topic (Kafka) / queue (RabbitMQ) for messages that cannot be processed. |
| **Reconciliation** | Comparing our records with the bank's to resolve unknown outcomes. |
| **Saga** | Sequence of local transactions with compensating actions instead of a distributed transaction. |

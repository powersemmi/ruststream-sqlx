-- The queue tables the checked structs describe, one per form and one for the headers layout.

-- Emails claimed by row lock: a group per name, a delayed retry, a counted attempt, a mark once
-- processed.
CREATE TABLE checked_emails (
    id           BIGSERIAL PRIMARY KEY,
    name         TEXT NOT NULL,
    retry_after  TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempt      SMALLINT NOT NULL DEFAULT 0,
    processed_at TIMESTAMPTZ,
    payload      BYTEA NOT NULL,
    note         TEXT NOT NULL
);

-- Ledger entries claimed by lease, in order within each account.
CREATE TABLE checked_ledger (
    id           BIGSERIAL PRIMARY KEY,
    account      TEXT NOT NULL,
    attempt      SMALLINT NOT NULL DEFAULT 0,
    locked_until TIMESTAMPTZ,
    payload      BYTEA NOT NULL
);

-- Webhooks held by an advisory lock on their endpoint.
CREATE TABLE checked_webhooks (
    id       BIGSERIAL PRIMARY KEY,
    endpoint TEXT NOT NULL,
    attempt  SMALLINT NOT NULL DEFAULT 0,
    payload  BYTEA NOT NULL
);

-- Orders described by a headers struct; the message reads its rows in its own fetch.
CREATE TABLE checked_orders (
    job_id       BIGSERIAL PRIMARY KEY,
    name         TEXT NOT NULL,
    processed_at TIMESTAMPTZ,
    trace        TEXT,
    total        BIGINT NOT NULL
);

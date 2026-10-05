-- The tables the live suites read and write. Each test runs in a database of its own.

-- The email queue: a group per name, every role of this phase.
CREATE TABLE email_jobs (
    job_id       BIGSERIAL PRIMARY KEY,
    name         TEXT NOT NULL,
    customer     TEXT,
    retry_after  TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempt      SMALLINT NOT NULL DEFAULT 1,
    processed_at TIMESTAMPTZ,
    meta         JSONB,
    payload      BYTEA NOT NULL
);

-- Where spent emails go when a registration dead-letters them into a table.
CREATE TABLE email_jobs_dead (LIKE email_jobs INCLUDING DEFAULTS);

-- One queue per table: no group, no time, rows deleted when finished.
CREATE TABLE plain_jobs (
    id      BIGSERIAL PRIMARY KEY,
    attempt SMALLINT NOT NULL DEFAULT 1,
    payload BYTEA NOT NULL
);

CREATE TABLE plain_jobs_dead (LIKE plain_jobs INCLUDING DEFAULTS);

-- A table whose struct names a column the table does not have.
CREATE TABLE broken_jobs (
    id      BIGSERIAL PRIMARY KEY,
    payload BYTEA NOT NULL
);

-- Jobs that point at orders; a custom fetch assembles the message from both.
CREATE TABLE orders (
    id   BIGINT PRIMARY KEY,
    body BYTEA NOT NULL
);

CREATE TABLE order_jobs (
    id       BIGSERIAL PRIMARY KEY,
    order_id BIGINT NOT NULL,
    done     BOOLEAN NOT NULL DEFAULT false
);

-- A queue on the database's clock.
CREATE TABLE clock_jobs (
    id           BIGSERIAL PRIMARY KEY,
    retry_after  TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempt      SMALLINT NOT NULL DEFAULT 1,
    processed_at TIMESTAMPTZ,
    payload      BYTEA NOT NULL
);

-- The conformance suites' tables: by-name subscriptions read the first, the lifecycle the second.
CREATE TABLE conformance_jobs (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    retry_after TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempt     SMALLINT NOT NULL DEFAULT 1,
    meta        JSONB,
    payload     BYTEA NOT NULL
);

CREATE TABLE lifecycle_jobs (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    retry_after TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempt     SMALLINT NOT NULL DEFAULT 1,
    payload     BYTEA NOT NULL
);

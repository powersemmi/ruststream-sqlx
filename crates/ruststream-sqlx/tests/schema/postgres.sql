-- The tables the live suites read and write. Each test runs in a database of its own.
--
-- A queue table the suites run in both forms carries a nullable `locked_until`: a lease writes it,
-- and the row lock form leaves it NULL.

-- The email queue: a group per name, every role of this phase.
CREATE TABLE email_jobs (
    job_id       BIGSERIAL PRIMARY KEY,
    name         TEXT NOT NULL,
    customer     TEXT,
    retry_after  TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempt      SMALLINT NOT NULL DEFAULT 1,
    processed_at TIMESTAMPTZ,
    locked_until TIMESTAMPTZ,
    meta         JSONB,
    payload      BYTEA NOT NULL
);

-- Where spent emails go when a registration dead-letters them into a table.
CREATE TABLE email_jobs_dead (LIKE email_jobs INCLUDING DEFAULTS);

-- A ledger whose accounts keep their order: a FIFO group per account, claimed by priority, then
-- by `retry_after`.
CREATE TABLE ledger (
    id           BIGSERIAL PRIMARY KEY,
    account      TEXT NOT NULL,
    priority     SMALLINT NOT NULL DEFAULT 0,
    retry_after  TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempt      SMALLINT NOT NULL DEFAULT 1,
    processed_at TIMESTAMPTZ,
    locked_until TIMESTAMPTZ,
    payload      BYTEA NOT NULL
);

-- One queue per table: no group, no time, rows deleted when finished. A job may name its tenant,
-- which an advisory lock key reads where the jobs of one tenant go one at a time.
CREATE TABLE plain_jobs (
    id           BIGSERIAL PRIMARY KEY,
    attempt      SMALLINT NOT NULL DEFAULT 1,
    tenant       TEXT NOT NULL DEFAULT '',
    locked_until TIMESTAMPTZ,
    payload      BYTEA NOT NULL
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
    id           BIGSERIAL PRIMARY KEY,
    name         TEXT NOT NULL,
    retry_after  TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempt      SMALLINT NOT NULL DEFAULT 1,
    locked_until TIMESTAMPTZ,
    meta         JSONB,
    payload      BYTEA NOT NULL
);

CREATE TABLE lifecycle_jobs (
    id           BIGSERIAL PRIMARY KEY,
    name         TEXT NOT NULL,
    retry_after  TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempt      SMALLINT NOT NULL DEFAULT 1,
    locked_until TIMESTAMPTZ,
    payload      BYTEA NOT NULL
);

-- Jobs another table may still point at: acknowledging a referenced job fails its statement.
CREATE TABLE fragile_jobs (
    id           BIGSERIAL PRIMARY KEY,
    locked_until TIMESTAMPTZ,
    payload      BYTEA NOT NULL
);

CREATE TABLE fragile_refs (
    job_id BIGINT NOT NULL REFERENCES fragile_jobs (id)
);

-- A payload column of the wrong type: the struct reads bytes, the table holds text.
CREATE TABLE mistyped_jobs (
    id      BIGSERIAL PRIMARY KEY,
    payload TEXT NOT NULL
);

-- Rows that decode or not by their own values: a struct field that takes no NULL.
CREATE TABLE partial_jobs (
    id      BIGSERIAL PRIMARY KEY,
    note    TEXT,
    payload BYTEA NOT NULL
);

-- An id column of the wrong type: nothing can settle such a row.
CREATE TABLE text_key_jobs (
    id      TEXT PRIMARY KEY,
    payload BYTEA NOT NULL
);

-- The id comes last, and the struct flattens another so its statements select `*`: the claim
-- finds the id of a row that does not decode by the column's name.
CREATE TABLE flat_jobs (
    note    TEXT,
    payload BYTEA NOT NULL,
    id      BIGSERIAL PRIMARY KEY
);

-- A queue whose acknowledgement is the service's own: it marks the row instead of deleting it.
-- Its lease form reads `locked_until`.
CREATE TABLE acked_jobs (
    id           BIGSERIAL PRIMARY KEY,
    acked        BOOLEAN NOT NULL DEFAULT false,
    locked_until TIMESTAMPTZ,
    payload      BYTEA NOT NULL
);

-- Role columns of the other types a by-name subscription reads: an INTEGER id and attempt, a byte
-- key and a text payload.
CREATE TABLE text_jobs (
    id      SERIAL PRIMARY KEY,
    tenant  BYTEA,
    attempt INTEGER NOT NULL DEFAULT 1,
    payload TEXT NOT NULL
);

-- `plain_jobs` with ids of text: each delivery's id has storage of its own.
CREATE TABLE keyed_jobs (
    id           TEXT PRIMARY KEY,
    attempt      SMALLINT NOT NULL DEFAULT 1,
    locked_until TIMESTAMPTZ,
    payload      BYTEA NOT NULL
);

-- A payload column of an integer, which a struct's bytes never read: its rows never decode. Its
-- lease form reads `locked_until`.
CREATE TABLE unreadable_jobs (
    id           BIGSERIAL PRIMARY KEY,
    attempt      SMALLINT NOT NULL DEFAULT 1,
    locked_until TIMESTAMPTZ,
    payload      BIGINT NOT NULL
);

-- Where spent jobs of `unreadable_jobs` go.
CREATE TABLE unreadable_jobs_dead (LIKE unreadable_jobs INCLUDING DEFAULTS);

-- Mails a handler takes as rows: no payload column, the recipient and the subject in columns of
-- their own. A recipient may be NULL, which a struct that reads it as text cannot read.
CREATE TABLE mail_jobs (
    job_id       BIGSERIAL PRIMARY KEY,
    name         TEXT NOT NULL,
    attempt      SMALLINT NOT NULL DEFAULT 1,
    locked_until TIMESTAMPTZ,
    meta         JSONB,
    recipient    TEXT,
    subject      TEXT
);

-- The isolation level each claim of a test's subscription ran at, written from inside the claim's
-- transaction.
CREATE TABLE seen_isolation (
    level TEXT NOT NULL
);

-- What a handler writes beside its job, in the delivery's transaction or through the pool: one
-- row per write, its note naming the write.
CREATE TABLE audit (
    job_id BIGINT,
    note   TEXT
);

-- A queue table whose message is assembled from it: the mechanics, the service's own headers
-- `tenant`, `trace` and `order_id`, and the job's note, which the message reads beside them.
CREATE TABLE headed_jobs (
    job_id       BIGSERIAL PRIMARY KEY,
    name         TEXT NOT NULL,
    attempt      SMALLINT NOT NULL DEFAULT 1,
    locked_until TIMESTAMPTZ,
    tenant       TEXT NOT NULL,
    trace        TEXT,
    order_id     BIGINT NOT NULL,
    note         TEXT
);

-- The orders the jobs of `headed_jobs` are for, which a fetch of the service's own joins.
CREATE TABLE customer_orders (
    id       BIGINT PRIMARY KEY,
    customer TEXT NOT NULL,
    total    BIGINT NOT NULL
);

-- The outbox of a service: what it published, kept until a consumer has processed it.
CREATE TABLE outbox (
    id           BIGSERIAL PRIMARY KEY,
    name         TEXT NOT NULL,
    payload      BYTEA NOT NULL,
    headers      JSONB,
    processed_at TIMESTAMPTZ
);

-- An outbox without `processed_at`: a processed record is deleted. `retries` counts what a
-- record's own retry event wrote, and refuses a second one, so a settlement can fail.
CREATE TABLE outbox_plain (
    id      BIGSERIAL PRIMARY KEY,
    name    TEXT NOT NULL,
    payload BYTEA NOT NULL,
    headers JSONB,
    retries INTEGER NOT NULL DEFAULT 0 CHECK (retries <= 1)
);

-- An outbox whose events are the service's own: its fetch takes a record by setting `taken_at`, a
-- retry releases the record and counts the attempt, a drop deletes it, and the recovery leaves a
-- taken record alone.
CREATE TABLE outbox_taken (
    id           BIGSERIAL PRIMARY KEY,
    name         TEXT NOT NULL,
    payload      BYTEA NOT NULL,
    headers      JSONB,
    taken_at     TIMESTAMPTZ,
    processed_at TIMESTAMPTZ,
    attempts     INTEGER NOT NULL DEFAULT 0
);

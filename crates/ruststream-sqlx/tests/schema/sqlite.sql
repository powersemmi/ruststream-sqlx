-- The tables the live suites read and write, in SQLite's types. Each test runs in an in-memory
-- database of its own.
--
-- Times are text in the layout sqlx writes `chrono` times in (`2026-10-05T12:00:00.250+00:00`),
-- which sorts as the times it holds; a default reads the clock in the same layout. A queue table
-- carries a nullable `locked_until`: SQLite claims rows by lease.

-- The email queue: a group per name, every role of this phase.
CREATE TABLE email_jobs (
    job_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    name         TEXT NOT NULL,
    customer     TEXT,
    retry_after  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')),
    attempt      INTEGER NOT NULL DEFAULT 1,
    processed_at TEXT,
    locked_until TEXT,
    meta         TEXT,
    payload      BLOB NOT NULL
);

-- Where spent emails go when a registration dead-letters them into a table.
CREATE TABLE email_jobs_dead (
    job_id       INTEGER PRIMARY KEY,
    name         TEXT NOT NULL,
    customer     TEXT,
    retry_after  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')),
    attempt      INTEGER NOT NULL DEFAULT 1,
    processed_at TEXT,
    locked_until TEXT,
    meta         TEXT,
    payload      BLOB NOT NULL
);

-- A ledger whose accounts keep their order: a FIFO group per account, claimed by priority, then
-- by `retry_after`.
CREATE TABLE ledger (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    account      TEXT NOT NULL,
    priority     INTEGER NOT NULL DEFAULT 0,
    retry_after  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')),
    attempt      INTEGER NOT NULL DEFAULT 1,
    processed_at TEXT,
    locked_until TEXT,
    payload      BLOB NOT NULL
);

-- One queue per table: no group, no time, rows deleted when finished. A job may name its tenant,
-- which an advisory lock key reads where the jobs of one tenant go one at a time.
CREATE TABLE plain_jobs (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    attempt      INTEGER NOT NULL DEFAULT 1,
    tenant       TEXT NOT NULL DEFAULT '',
    locked_until TEXT,
    payload      BLOB NOT NULL
);

CREATE TABLE plain_jobs_dead (
    id           INTEGER PRIMARY KEY,
    attempt      INTEGER NOT NULL DEFAULT 1,
    tenant       TEXT NOT NULL DEFAULT '',
    locked_until TEXT,
    payload      BLOB NOT NULL
);

-- A table whose struct names a column the table does not have.
CREATE TABLE broken_jobs (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    payload BLOB NOT NULL
);

-- Jobs that point at orders; a custom fetch assembles the message from both.
CREATE TABLE orders (
    id   INTEGER PRIMARY KEY,
    body BLOB NOT NULL
);

CREATE TABLE order_jobs (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id INTEGER NOT NULL,
    done     BOOLEAN NOT NULL DEFAULT 0
);

-- A queue on the database's clock.
CREATE TABLE clock_jobs (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    retry_after  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')),
    attempt      INTEGER NOT NULL DEFAULT 1,
    processed_at TEXT,
    payload      BLOB NOT NULL
);

-- The conformance suites' tables: by-name subscriptions read the first, the lifecycle the second.
CREATE TABLE conformance_jobs (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    name         TEXT NOT NULL,
    retry_after  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')),
    attempt      INTEGER NOT NULL DEFAULT 1,
    locked_until TEXT,
    meta         TEXT,
    payload      BLOB NOT NULL
);

CREATE TABLE lifecycle_jobs (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    name         TEXT NOT NULL,
    retry_after  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')),
    attempt      INTEGER NOT NULL DEFAULT 1,
    locked_until TEXT,
    payload      BLOB NOT NULL
);

-- Jobs another table may still point at: acknowledging a referenced job fails its statement (sqlx
-- turns foreign keys on for every connection).
CREATE TABLE fragile_jobs (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    locked_until TEXT,
    payload      BLOB NOT NULL
);

CREATE TABLE fragile_refs (
    job_id INTEGER NOT NULL REFERENCES fragile_jobs (id)
);

-- A payload column of text, where the struct reads bytes.
CREATE TABLE mistyped_jobs (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    payload TEXT NOT NULL
);

-- Rows that decode or not by their own values: a struct field that takes no NULL.
CREATE TABLE partial_jobs (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    note    TEXT,
    payload BLOB NOT NULL
);

-- An id column of text, where the struct reads an integer.
CREATE TABLE text_key_jobs (
    id      TEXT PRIMARY KEY,
    payload BLOB NOT NULL
);

-- The id comes last, and the struct flattens another so its statements select `*`: the claim
-- finds the id of a row that does not decode by the column's name.
CREATE TABLE flat_jobs (
    note    TEXT,
    payload BLOB NOT NULL,
    id      INTEGER PRIMARY KEY AUTOINCREMENT
);

-- A queue whose acknowledgement is the service's own: it marks the row instead of deleting it.
-- Its lease form reads `locked_until`.
CREATE TABLE acked_jobs (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    acked        BOOLEAN NOT NULL DEFAULT 0,
    locked_until TEXT,
    payload      BLOB NOT NULL
);

-- Role columns of the other types a by-name subscription reads: a byte key and a text payload.
CREATE TABLE text_jobs (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant  BLOB,
    attempt INTEGER NOT NULL DEFAULT 1,
    payload TEXT NOT NULL
);

-- `plain_jobs` with ids of text: each delivery's id has storage of its own.
CREATE TABLE keyed_jobs (
    id           TEXT PRIMARY KEY,
    attempt      INTEGER NOT NULL DEFAULT 1,
    locked_until TEXT,
    payload      BLOB NOT NULL
);

-- A payload column of an integer, which a struct's bytes never read: its rows never decode.
CREATE TABLE unreadable_jobs (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    attempt      INTEGER NOT NULL DEFAULT 1,
    locked_until TEXT,
    payload      INTEGER NOT NULL
);

-- Where spent jobs of `unreadable_jobs` go.
CREATE TABLE unreadable_jobs_dead (
    id           INTEGER PRIMARY KEY,
    attempt      INTEGER NOT NULL DEFAULT 1,
    locked_until TEXT,
    payload      INTEGER NOT NULL
);

-- Mails a handler takes as rows: no payload column, the recipient and the subject in columns of
-- their own. A recipient may hold bytes that are not text, which a struct that reads it as text
-- cannot read.
CREATE TABLE mail_jobs (
    job_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    name         TEXT NOT NULL,
    attempt      INTEGER NOT NULL DEFAULT 1,
    locked_until TEXT,
    meta         TEXT,
    recipient    TEXT,
    subject      TEXT
);

-- What a handler writes beside its job, in the delivery's transaction or through the pool: one
-- row per write, its note naming the write.
CREATE TABLE audit (
    job_id BIGINT,
    note   TEXT
);

-- The queue tables the checked structs describe, one per form SQLite claims rows in. Times are text
-- in the layout sqlx writes `chrono` times in, which sorts as the times it holds.

-- Ledger entries claimed by lease, in order within each account.
CREATE TABLE checked_ledger (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    account      TEXT NOT NULL,
    attempt      INTEGER NOT NULL DEFAULT 0,
    locked_until TEXT,
    payload      BLOB NOT NULL
);

-- Webhooks held by an advisory lock on their endpoint.
CREATE TABLE checked_webhooks (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    endpoint TEXT NOT NULL,
    attempt  INTEGER NOT NULL DEFAULT 0,
    payload  BLOB NOT NULL
);

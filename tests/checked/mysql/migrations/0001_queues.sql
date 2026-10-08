-- The queue tables the checked structs describe, one per form.

-- Emails claimed by row lock: a group per name, a delayed retry, a counted attempt, a mark once
-- processed.
CREATE TABLE checked_emails (
    id           BIGINT AUTO_INCREMENT PRIMARY KEY,
    name         VARCHAR(255) NOT NULL,
    retry_after  DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    attempt      SMALLINT NOT NULL DEFAULT 0,
    processed_at DATETIME(6),
    payload      LONGBLOB NOT NULL,
    note         TEXT NOT NULL
);

-- Ledger entries claimed by lease, in order within each account. A lease ends on a whole second,
-- which a DATETIME without fractions holds exactly.
CREATE TABLE checked_ledger (
    id           BIGINT AUTO_INCREMENT PRIMARY KEY,
    account      VARCHAR(255) NOT NULL,
    attempt      SMALLINT NOT NULL DEFAULT 0,
    locked_until DATETIME,
    payload      LONGBLOB NOT NULL
);

-- Webhooks held by an advisory lock on their endpoint.
CREATE TABLE checked_webhooks (
    id       BIGINT AUTO_INCREMENT PRIMARY KEY,
    endpoint VARCHAR(255) NOT NULL,
    attempt  SMALLINT NOT NULL DEFAULT 0,
    payload  LONGBLOB NOT NULL
);

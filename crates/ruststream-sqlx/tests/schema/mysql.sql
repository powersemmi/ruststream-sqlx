-- The tables the live suites read and write, in MySQL's and MariaDB's types. Each test runs in a
-- database of its own.
--
-- A queue table the suites run in both forms carries a nullable `locked_until`: a lease writes it,
-- and the row lock form leaves it NULL. It is a DATETIME without fractions: a lease ends on a
-- whole second, which such a column holds exactly.

-- The email queue: a group per name, every role of this phase.
CREATE TABLE email_jobs (
    job_id       BIGINT AUTO_INCREMENT PRIMARY KEY,
    name         VARCHAR(255) NOT NULL,
    customer     VARCHAR(255),
    retry_after  DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    attempt      SMALLINT NOT NULL DEFAULT 1,
    processed_at DATETIME(6),
    locked_until DATETIME,
    meta         JSON,
    payload      LONGBLOB NOT NULL
);

-- Where spent emails go when a registration dead-letters them into a table.
CREATE TABLE email_jobs_dead LIKE email_jobs;

-- A ledger whose accounts keep their order: a FIFO group per account, claimed by priority, then
-- by `retry_after`.
CREATE TABLE ledger (
    id           BIGINT AUTO_INCREMENT PRIMARY KEY,
    account      VARCHAR(255) NOT NULL,
    priority     SMALLINT NOT NULL DEFAULT 0,
    retry_after  DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    attempt      SMALLINT NOT NULL DEFAULT 1,
    processed_at DATETIME(6),
    locked_until DATETIME,
    payload      LONGBLOB NOT NULL
);

-- A locking read whose order no index serves locks every row it reads, so without this index two
-- claims of one group would wait on each other whether the group keeps its order or not.
CREATE INDEX ledger_order ON ledger (account, priority, retry_after, id);

-- One queue per table: no group, no time, rows deleted when finished. A job may name its tenant,
-- which an advisory lock key reads where the jobs of one tenant go one at a time.
CREATE TABLE plain_jobs (
    id           BIGINT AUTO_INCREMENT PRIMARY KEY,
    attempt      SMALLINT NOT NULL DEFAULT 1,
    tenant       VARCHAR(64) NOT NULL DEFAULT '',
    locked_until DATETIME,
    payload      LONGBLOB NOT NULL
);

CREATE TABLE plain_jobs_dead LIKE plain_jobs;

-- A table whose struct names a column the table does not have.
CREATE TABLE broken_jobs (
    id      BIGINT AUTO_INCREMENT PRIMARY KEY,
    payload LONGBLOB NOT NULL
);

-- Jobs that point at orders; a custom fetch assembles the message from both.
CREATE TABLE orders (
    id   BIGINT PRIMARY KEY,
    body LONGBLOB NOT NULL
);

CREATE TABLE order_jobs (
    id       BIGINT AUTO_INCREMENT PRIMARY KEY,
    order_id BIGINT NOT NULL,
    done     BOOLEAN NOT NULL DEFAULT false
);

-- A queue on the database's clock.
CREATE TABLE clock_jobs (
    id           BIGINT AUTO_INCREMENT PRIMARY KEY,
    retry_after  DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    attempt      SMALLINT NOT NULL DEFAULT 1,
    processed_at DATETIME(6),
    payload      LONGBLOB NOT NULL
);

-- The conformance suites' tables: by-name subscriptions read the first, the lifecycle the second.
CREATE TABLE conformance_jobs (
    id           BIGINT AUTO_INCREMENT PRIMARY KEY,
    name         VARCHAR(255) NOT NULL,
    retry_after  DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    attempt      SMALLINT NOT NULL DEFAULT 1,
    locked_until DATETIME,
    meta         JSON,
    payload      LONGBLOB NOT NULL
);

CREATE TABLE lifecycle_jobs (
    id           BIGINT AUTO_INCREMENT PRIMARY KEY,
    name         VARCHAR(255) NOT NULL,
    retry_after  DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    attempt      SMALLINT NOT NULL DEFAULT 1,
    locked_until DATETIME,
    payload      LONGBLOB NOT NULL
);
-- The claim order, which keeps a claim to the rows it takes: without it a claim locks every row
-- of its group it reads.
CREATE INDEX lifecycle_order ON lifecycle_jobs (name, retry_after, id);

-- Jobs another table may still point at: acknowledging a referenced job fails its statement.
CREATE TABLE fragile_jobs (
    id           BIGINT AUTO_INCREMENT PRIMARY KEY,
    locked_until DATETIME,
    payload      LONGBLOB NOT NULL
);

CREATE TABLE fragile_refs (
    job_id BIGINT NOT NULL,
    FOREIGN KEY (job_id) REFERENCES fragile_jobs (id)
);

-- A payload column of text, where the struct reads bytes.
CREATE TABLE mistyped_jobs (
    id      BIGINT AUTO_INCREMENT PRIMARY KEY,
    payload TEXT NOT NULL
);

-- Rows that decode or not by their own values: a struct field that takes no NULL.
CREATE TABLE partial_jobs (
    id      BIGINT AUTO_INCREMENT PRIMARY KEY,
    note    TEXT,
    payload LONGBLOB NOT NULL
);

-- An id column of the wrong type: nothing can settle such a row.
CREATE TABLE text_key_jobs (
    id      VARCHAR(255) PRIMARY KEY,
    payload LONGBLOB NOT NULL
);

-- The id comes last, and the struct flattens another so its statements select `*`: the claim
-- finds the id of a row that does not decode by the column's name.
CREATE TABLE flat_jobs (
    note    TEXT,
    payload LONGBLOB NOT NULL,
    id      BIGINT AUTO_INCREMENT PRIMARY KEY
);

-- A queue whose acknowledgement is the service's own: it marks the row instead of deleting it.
-- Its lease form reads `locked_until`.
CREATE TABLE acked_jobs (
    id           BIGINT AUTO_INCREMENT PRIMARY KEY,
    acked        BOOLEAN NOT NULL DEFAULT false,
    locked_until DATETIME,
    payload      LONGBLOB NOT NULL
);

-- Role columns of the other types a by-name subscription reads: an INT id and attempt, a byte key
-- and a text payload.
CREATE TABLE text_jobs (
    id      INT AUTO_INCREMENT PRIMARY KEY,
    tenant  VARBINARY(255),
    attempt INT NOT NULL DEFAULT 1,
    payload TEXT NOT NULL
);

-- `plain_jobs` with ids of text: each delivery's id has storage of its own.
CREATE TABLE keyed_jobs (
    id           VARCHAR(255) PRIMARY KEY,
    attempt      SMALLINT NOT NULL DEFAULT 1,
    locked_until DATETIME,
    payload      LONGBLOB NOT NULL
);

-- A payload column of an integer, which a struct's bytes never read: its rows never decode. Its
-- lease form reads `locked_until`.
CREATE TABLE unreadable_jobs (
    id           BIGINT AUTO_INCREMENT PRIMARY KEY,
    attempt      SMALLINT NOT NULL DEFAULT 1,
    locked_until DATETIME,
    payload      BIGINT NOT NULL
);

-- Where spent jobs of `unreadable_jobs` go.
CREATE TABLE unreadable_jobs_dead LIKE unreadable_jobs;

-- Mails a handler takes as rows: no payload column, the recipient and the subject in columns of
-- their own. A recipient may be NULL, which a struct that reads it as text cannot read.
CREATE TABLE mail_jobs (
    job_id       BIGINT AUTO_INCREMENT PRIMARY KEY,
    name         VARCHAR(255) NOT NULL,
    attempt      SMALLINT NOT NULL DEFAULT 1,
    locked_until DATETIME,
    meta         JSON,
    recipient    VARCHAR(255),
    subject      VARCHAR(255)
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
    job_id       BIGINT AUTO_INCREMENT PRIMARY KEY,
    name         VARCHAR(255) NOT NULL,
    attempt      SMALLINT NOT NULL DEFAULT 1,
    locked_until DATETIME,
    tenant       VARCHAR(255) NOT NULL,
    trace        VARCHAR(255),
    order_id     BIGINT NOT NULL,
    note         VARCHAR(255)
);

-- The orders the jobs of `headed_jobs` are for, which a fetch of the service's own joins.
CREATE TABLE customer_orders (
    id       BIGINT PRIMARY KEY,
    customer VARCHAR(255) NOT NULL,
    total    BIGINT NOT NULL
);

-- The outbox of a service: what it published, kept until a consumer has processed it.
CREATE TABLE outbox (
    id           BIGINT AUTO_INCREMENT PRIMARY KEY,
    name         VARCHAR(255) NOT NULL,
    payload      LONGBLOB NOT NULL,
    headers      JSON,
    processed_at DATETIME(6)
);

-- An outbox without `processed_at`: a processed record is deleted. `retries` counts what a
-- record's own retry event wrote, and refuses a second one, so a settlement can fail.
CREATE TABLE outbox_plain (
    id      BIGINT AUTO_INCREMENT PRIMARY KEY,
    name    VARCHAR(255) NOT NULL,
    payload LONGBLOB NOT NULL,
    headers JSON,
    retries INT NOT NULL DEFAULT 0 CHECK (retries <= 1)
);

-- An outbox whose events are the service's own: its fetch takes a record by setting `taken_at`, a
-- retry releases the record and counts the attempt, a drop deletes it, and the recovery leaves a
-- taken record alone.
CREATE TABLE outbox_taken (
    id           BIGINT AUTO_INCREMENT PRIMARY KEY,
    name         VARCHAR(255) NOT NULL,
    payload      LONGBLOB NOT NULL,
    headers      JSON,
    taken_at     DATETIME(6),
    processed_at DATETIME(6),
    attempts     INT NOT NULL DEFAULT 0
);

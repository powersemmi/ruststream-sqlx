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

-- One queue per table: no group, no time, rows deleted when finished.
CREATE TABLE plain_jobs (
    id           BIGINT AUTO_INCREMENT PRIMARY KEY,
    attempt      SMALLINT NOT NULL DEFAULT 1,
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

-- The service owns the table and creates it with its own sqlx migrations.
CREATE TABLE outbox (
    id           uuid PRIMARY KEY,
    name         text NOT NULL,
    payload      bytea NOT NULL,
    taken_at     timestamptz,
    processed_at timestamptz
);

CREATE INDEX outbox_unprocessed ON outbox (id) WHERE processed_at IS NULL;

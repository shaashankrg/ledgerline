-- Baseline migration. Proves Flyway connects and can write to Postgres.
-- Business tables land in later migrations.
CREATE TABLE app_metadata (
    id          SMALLINT PRIMARY KEY,
    schema_name TEXT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO app_metadata (id, schema_name) VALUES (1, 'ledgerline');

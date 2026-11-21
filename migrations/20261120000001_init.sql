-- Hooklane schema: seven tables.
--
-- The unique constraints and partial indexes here are not decoration. Several of
-- the delivery guarantees depend on them:
--   * events (tenant_id, idempotency_key) unique  -> a retried POST /v1/events
--     with the same Idempotency-Key cannot create a second event, even when the
--     two requests race each other.
--   * deliveries (event_id, endpoint_id) unique   -> an event can never be fanned
--     out twice to the same endpoint.
--   * the partial indexes keep the "find work" queries (due retries, stuck
--     deliveries, pending work, unpublished outbox rows) fast no matter how many
--     finished rows pile up, because finished rows are simply not in the index.

CREATE TABLE tenants (
    id          uuid PRIMARY KEY,
    name        text NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE api_keys (
    id          uuid PRIMARY KEY,
    tenant_id   uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    -- SHA-256 of the full key. Keys are 32 random bytes, so a fast hash is fine:
    -- there is nothing to brute-force the way there is with a human password.
    key_hash    bytea NOT NULL UNIQUE,
    prefix      text NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    revoked_at  timestamptz
);
CREATE INDEX api_keys_tenant_idx ON api_keys (tenant_id);

CREATE TABLE endpoints (
    id                          uuid PRIMARY KEY,
    tenant_id                   uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    url                         text NOT NULL,
    description                 text NOT NULL DEFAULT '',
    -- NULL or empty array = subscribed to every (non-system) event type.
    event_types                 text[],
    secret_ciphertext           bytea NOT NULL,
    previous_secret_ciphertext  bytea,
    previous_secret_expires_at  timestamptz,
    status                      text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'disabled')),
    disabled_reason             text,
    rate_limit_per_sec          int NOT NULL DEFAULT 50 CHECK (rate_limit_per_sec > 0),
    failing_since               timestamptz,
    created_at                  timestamptz NOT NULL DEFAULT now(),
    updated_at                  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX endpoints_tenant_idx ON endpoints (tenant_id, id);
-- Used by the auto-disable loop.
CREATE INDEX endpoints_failing_idx ON endpoints (failing_since) WHERE failing_since IS NOT NULL AND status = 'active';

CREATE TABLE events (
    id               uuid PRIMARY KEY,           -- UUIDv7, so ids sort by creation time
    tenant_id        uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    event_type       text NOT NULL,
    payload          jsonb NOT NULL,
    idempotency_key  text,
    created_at       timestamptz NOT NULL DEFAULT now()
);
-- The idempotency guarantee. Enforced by the database, not by a "check then
-- insert" in application code, because two concurrent requests can both pass
-- the check before either inserts.
CREATE UNIQUE INDEX events_idempotency_idx ON events (tenant_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX events_tenant_idx ON events (tenant_id, id);

CREATE TABLE deliveries (
    id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id         uuid NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    endpoint_id      uuid NOT NULL REFERENCES endpoints(id) ON DELETE CASCADE,
    tenant_id        uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    status           text NOT NULL DEFAULT 'pending'
                     CHECK (status IN ('pending', 'delivering', 'succeeded', 'retry_scheduled', 'failed', 'dead')),
    attempt_count    int NOT NULL DEFAULT 0,
    next_attempt_at  timestamptz,
    claimed_at       timestamptz,
    last_error       text,
    completed_at     timestamptz,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    -- One delivery per (event, endpoint): fan-out can never double up.
    CONSTRAINT deliveries_event_endpoint_key UNIQUE (event_id, endpoint_id)
);
CREATE INDEX deliveries_retry_due_idx  ON deliveries (next_attempt_at) WHERE status = 'retry_scheduled';
CREATE INDEX deliveries_delivering_idx ON deliveries (claimed_at)      WHERE status = 'delivering';
CREATE INDEX deliveries_pending_idx    ON deliveries (id)              WHERE status = 'pending';
CREATE INDEX deliveries_endpoint_idx   ON deliveries (endpoint_id, id);
CREATE INDEX deliveries_tenant_idx     ON deliveries (tenant_id, id);

CREATE TABLE attempts (
    id                     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    delivery_id            bigint NOT NULL REFERENCES deliveries(id) ON DELETE CASCADE,
    attempt_number         int NOT NULL,
    started_at             timestamptz NOT NULL,
    duration_ms            int NOT NULL,
    response_status        int,
    response_body_snippet  text,   -- first 1 KB only
    error_kind             text,
    error                  text
);
CREATE INDEX attempts_delivery_idx ON attempts (delivery_id);

CREATE TABLE outbox (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    delivery_id   bigint NOT NULL REFERENCES deliveries(id) ON DELETE CASCADE,
    endpoint_id   uuid NOT NULL,
    tenant_id     uuid NOT NULL,
    traceparent   text,
    created_at    timestamptz NOT NULL DEFAULT now(),
    published_at  timestamptz
);
-- The relay only ever looks at unpublished rows; published ones drop out of this index.
CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;
CREATE INDEX outbox_published_idx ON outbox (published_at) WHERE published_at IS NOT NULL;

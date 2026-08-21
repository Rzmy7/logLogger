-- 001_initial_schema.sql
-- Initial schema migration for Log Analytics Platform metadata

-- 1. Applications table
CREATE TABLE IF NOT EXISTS applications (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        VARCHAR(100) NOT NULL,
    description TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_applications_name UNIQUE (name)
);

CREATE INDEX IF NOT EXISTS idx_applications_name ON applications(name);

-- 2. Environments table
CREATE TABLE IF NOT EXISTS environments (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name       VARCHAR(50) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_environments_name UNIQUE (name)
);

-- Default environments seed
INSERT INTO environments (name)
VALUES ('production'), ('staging'), ('development')
ON CONFLICT (name) DO NOTHING;

-- 3. Services table
CREATE TABLE IF NOT EXISTS services (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    application_id  UUID NOT NULL REFERENCES applications(id) ON DELETE CASCADE,
    environment_id  UUID NOT NULL REFERENCES environments(id) ON DELETE CASCADE,
    name            VARCHAR(100) NOT NULL,
    description     TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_services_app_env_name UNIQUE (application_id, environment_id, name)
);

CREATE INDEX IF NOT EXISTS idx_services_app_id ON services(application_id);
CREATE INDEX IF NOT EXISTS idx_services_env_id ON services(environment_id);
CREATE INDEX IF NOT EXISTS idx_services_name ON services(name);

-- 4. Alert Rules table
CREATE TABLE IF NOT EXISTS alert_rules (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    service_id     UUID NOT NULL REFERENCES services(id) ON DELETE CASCADE,
    name           VARCHAR(200) NOT NULL,
    condition      VARCHAR(50) NOT NULL,
    threshold      INT NOT NULL,
    window_minutes INT NOT NULL DEFAULT 5,
    enabled        BOOLEAN NOT NULL DEFAULT true,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alert_rules_service ON alert_rules(service_id);
CREATE INDEX IF NOT EXISTS idx_alert_rules_enabled ON alert_rules(enabled);

-- 5. Saved Searches table
CREATE TABLE IF NOT EXISTS saved_searches (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name       VARCHAR(200) NOT NULL,
    query      JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_saved_searches_name ON saved_searches(name);

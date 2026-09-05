-- GoFlow initial schema.
--
-- Design notes:
--   * jobs.status is a TEXT enum enforced with a CHECK constraint rather than
--     a separate lookup table -- there are only 8 fixed values and this
--     keeps reads simple.
--   * job_attempts is an append-only audit log of every execution attempt
--     (including timeouts and cancellations), independent of the mutable
--     current state on jobs. This is what "Design > Challenges" in the
--     README leans on when explaining how retry history is inspected.
--   * worker_heartbeats is an append-only log capped by application-level
--     pruning (see internal/database/workers.go) so it stays small while
--     still giving a short recent history per worker, not just a single
--     "last seen" timestamp.

CREATE TABLE IF NOT EXISTS users (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    username   TEXT NOT NULL UNIQUE,
    api_key    TEXT NOT NULL UNIQUE,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS jobs (
    id             TEXT PRIMARY KEY,
    type           TEXT NOT NULL,
    payload        BLOB NOT NULL DEFAULT '',
    priority       TEXT NOT NULL CHECK (priority IN ('HIGH','MEDIUM','LOW')),
    status         TEXT NOT NULL CHECK (status IN
                     ('PENDING','SCHEDULED','RUNNING','COMPLETED','FAILED',
                      'RETRYING','CANCELLED','DEAD')),
    created_at     DATETIME NOT NULL,
    started_at     DATETIME,
    completed_at   DATETIME,
    scheduled_at   DATETIME,
    retry_count    INTEGER NOT NULL DEFAULT 0,
    max_retries    INTEGER NOT NULL DEFAULT 3,
    timeout_secs   INTEGER NOT NULL DEFAULT 30,
    error_message  TEXT NOT NULL DEFAULT '',
    worker_id      TEXT,
    created_by     INTEGER REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_priority_status ON jobs(status, priority);
CREATE INDEX IF NOT EXISTS idx_jobs_scheduled_at ON jobs(scheduled_at) WHERE scheduled_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs(created_at);

CREATE TABLE IF NOT EXISTS job_attempts (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id         TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt_number INTEGER NOT NULL,
    worker_id      TEXT NOT NULL,
    started_at     DATETIME NOT NULL,
    completed_at   DATETIME,
    status         TEXT NOT NULL,
    error_message  TEXT NOT NULL DEFAULT '',
    duration_ms    INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_job_attempts_job_id ON job_attempts(job_id);

CREATE TABLE IF NOT EXISTS workers (
    id              TEXT PRIMARY KEY,
    status          TEXT NOT NULL,
    current_job_id  TEXT,
    jobs_processed  INTEGER NOT NULL DEFAULT 0,
    jobs_failed     INTEGER NOT NULL DEFAULT 0,
    started_at      DATETIME NOT NULL,
    last_heartbeat  DATETIME NOT NULL,
    stopped_at      DATETIME
);

CREATE TABLE IF NOT EXISTS worker_heartbeats (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    worker_id     TEXT NOT NULL REFERENCES workers(id) ON DELETE CASCADE,
    heartbeat_at  DATETIME NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_worker_heartbeats_worker_id ON worker_heartbeats(worker_id, heartbeat_at);

INSERT OR IGNORE INTO users (id, username, api_key)
VALUES (1, 'system', 'local-dev-key-not-a-secret');

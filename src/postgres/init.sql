-- Runs automatically when the postgres container starts for the first time.
-- Mounted via docker-compose: ./postgres/init.sql → /docker-entrypoint-initdb.d/

CREATE TABLE IF NOT EXISTS violations (
    id            SERIAL PRIMARY KEY,
    violation_id  UUID          NOT NULL UNIQUE,
    frame_id      INTEGER       NOT NULL,
    track_id      INTEGER       NOT NULL,
    roi_id        TEXT          NOT NULL,
    frame_path    TEXT          NOT NULL,
    timestamp     DOUBLE PRECISION NOT NULL,
    created_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_violations_timestamp  ON violations (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_violations_track_id   ON violations (track_id);
CREATE INDEX IF NOT EXISTS idx_violations_roi_id     ON violations (roi_id);
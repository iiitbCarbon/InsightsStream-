-- InsightsStream serving schema
CREATE TABLE IF NOT EXISTS metric_aggregates (
    metric        TEXT        NOT NULL,
    window_start  TIMESTAMPTZ NOT NULL,
    total         BIGINT      NOT NULL,
    PRIMARY KEY (metric, window_start)
);

-- Composite index drives the API's primary read path
CREATE INDEX IF NOT EXISTS idx_metric_window
    ON metric_aggregates (metric, window_start DESC);

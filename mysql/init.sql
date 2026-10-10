-- Created on first MySQL container start (mounted into /docker-entrypoint-initdb.d/).
-- Column names and types must match the DataFrame projected by pyspark-worker.py;
-- The worker inserts by column name and will fail on a mismatch.
-- review_hash is SHA-256 of the raw Kafka message; the unique key makes
-- inserts idempotent so replays and duplicate source records are stored once.

CREATE TABLE IF NOT EXISTS processed_reviews (
    id                   BIGINT       NOT NULL AUTO_INCREMENT,
    review_hash          CHAR(64)     NOT NULL,
    event_timestamp      TIMESTAMP    NULL,
    overall              DOUBLE       NULL,
    verified             BOOLEAN      NULL,
    review_time          VARCHAR(32)  NULL,
    reviewer_id          VARCHAR(64)  NULL,
    asin                 VARCHAR(32)  NULL,
    reviewer_name        VARCHAR(255) NULL,
    review_text          TEXT         NULL,
    summary              TEXT         NULL,
    unix_review_time     BIGINT       NULL,
    text_lower           TEXT         NULL,
    tokens               MEDIUMTEXT   NULL,
    tokens_no_stopwords  MEDIUMTEXT   NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_review_hash (review_hash),
    INDEX idx_event_timestamp (event_timestamp),
    INDEX idx_asin (asin)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

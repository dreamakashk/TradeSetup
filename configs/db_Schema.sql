-- Active: 1751093929180@@127.0.0.1@5432@trade_setup
-- Master table for the Stock Market Data store. This table contains list of all stocks which is important for the system.
-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- 1. Table: Stocks (metadata)
CREATE TABLE stocks (
    ticker            VARCHAR(16)      PRIMARY KEY,
    company_name      TEXT,
    listing_date      DATE,
    sector            VARCHAR(32),
    industry          VARCHAR(32),
    is_active         BOOLEAN          NOT NULL DEFAULT TRUE,
    is_index          BOOLEAN          NOT NULL DEFAULT FALSE,
    last_price_fetch  DATE,
    last_fund_fetch   DATE,
    created_at        TIMESTAMPTZ      NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ      NOT NULL DEFAULT now()
);

CREATE INDEX idx_stocks_sector   ON stocks(sector);
CREATE INDEX idx_stocks_industry ON stocks(industry);

-- 2. Table: Stock Price Daily (hypertable)
CREATE TABLE stock_price_daily (
    ticker        VARCHAR(16) NOT NULL REFERENCES stocks(ticker),
    date          DATE        NOT NULL,
    open          DOUBLE PRECISION,
    high          DOUBLE PRECISION,
    low           DOUBLE PRECISION,
    close         DOUBLE PRECISION,
    volume        BIGINT,
    dividend      DOUBLE PRECISION DEFAULT 0,
    split_factor  DOUBLE PRECISION DEFAULT 0,
    created       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (ticker, date)
);

-- Make stock_price_daily a hypertable with 1-month chunking and space partitioning by ticker
SELECT create_hypertable(
    'stock_price_daily',
    'date',
    'ticker',
    2,
    chunk_time_interval => INTERVAL '1 month'
);

-- Enable compression and set compression policy for data older than 1 year
ALTER TABLE stock_price_daily SET (timescaledb.compress, timescaledb.compress_segmentby = 'ticker');
SELECT add_compression_policy('stock_price_daily', INTERVAL '1 year');

-- 3. Table: Stock Indicators Daily (hypertable)
CREATE TABLE stock_indicators_daily (
    ticker        VARCHAR(16) NOT NULL REFERENCES stocks(ticker),
    date          DATE        NOT NULL,
    ema_10        DOUBLE PRECISION,
    ema_20        DOUBLE PRECISION,
    ema_50        DOUBLE PRECISION,
    ema_100       DOUBLE PRECISION,
    ema_200       DOUBLE PRECISION,
    rsi           DOUBLE PRECISION,
    atr           DOUBLE PRECISION,
    supertrend    DOUBLE PRECISION,
    obv           DOUBLE PRECISION,
    ad            DOUBLE PRECISION,
    volume_surge  DOUBLE PRECISION,
    PRIMARY KEY (ticker, date)
);

-- Make stock_indicators_daily a hypertable with 1-month chunking and space partitioning by ticker
SELECT create_hypertable(
    'stock_indicators_daily',
    'date',
    'ticker',
    2,
    chunk_time_interval => INTERVAL '1 month'
);

-- Enable compression and set compression policy for data older than 1 year
ALTER TABLE stock_indicators_daily SET (timescaledb.compress, timescaledb.compress_segmentby = 'ticker');
SELECT add_compression_policy('stock_indicators_daily', INTERVAL '1 year');

—----------------------------------------------------
-- WEEKLY TABLES

-- 1. Weekly Price Table (Hypertable)
CREATE TABLE stock_price_weekly (
    ticker        VARCHAR(16) NOT NULL REFERENCES stocks(ticker),
    week_start    DATE        NOT NULL, -- The Monday of the week
    open          DOUBLE PRECISION,
    high          DOUBLE PRECISION,
    low           DOUBLE PRECISION,
    close         DOUBLE PRECISION,
    volume        BIGINT,
    dividend      DOUBLE PRECISION DEFAULT 0,
    split_factor  DOUBLE PRECISION DEFAULT 0,
    created       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (ticker, week_start)
);

-- Convert to hypertable
SELECT create_hypertable(
    'stock_price_weekly',
    'week_start',
    'ticker',
    2,
    chunk_time_interval => INTERVAL '1 month'
);

-- 2. Weekly Indicators Table (Hypertable)
CREATE TABLE stock_indicators_weekly (
    ticker        VARCHAR(16) NOT NULL REFERENCES stocks(ticker),
    week_start    DATE        NOT NULL,
    ema_10        DOUBLE PRECISION,  -- 10-week EMA (≈50-day EMA)
    ema_40        DOUBLE PRECISION,  -- 40-week EMA (≈200-day EMA)
    rsi           DOUBLE PRECISION,
    atr           DOUBLE PRECISION,
    supertrend    DOUBLE PRECISION,
    obv           DOUBLE PRECISION,
    ad            DOUBLE PRECISION,
    volume_surge  DOUBLE PRECISION,
    PRIMARY KEY (ticker, week_start)
);

-- Convert to hypertable
SELECT create_hypertable(
    'stock_indicators_weekly',
    'week_start',
    'ticker',
    2,
    chunk_time_interval => INTERVAL '1 month'
);

-- Enable compression for older data if desired
ALTER TABLE stock_price_weekly SET (timescaledb.compress, timescaledb.compress_segmentby = 'ticker');
SELECT add_compression_policy('stock_price_weekly', INTERVAL '1 year');

ALTER TABLE stock_indicators_weekly SET (timescaledb.compress, timescaledb.compress_segmentby = 'ticker');
SELECT add_compression_policy('stock_indicators_weekly', INTERVAL '1 year');


-- MONTHLY TABLES

-- 1. Monthly Price Table (Hypertable)
CREATE TABLE stock_price_monthly (
    ticker        VARCHAR(16) NOT NULL REFERENCES stocks(ticker),
    month_start   DATE        NOT NULL, -- The first day of the month
    open          DOUBLE PRECISION,
    high          DOUBLE PRECISION,
    low           DOUBLE PRECISION,
    close         DOUBLE PRECISION,
    volume        BIGINT,
    dividend      DOUBLE PRECISION DEFAULT 0,
    split_factor  DOUBLE PRECISION DEFAULT 0,
    created       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (ticker, month_start)
);

-- Convert to hypertable with 3-month chunk interval (recommended for monthly data)
SELECT create_hypertable(
    'stock_price_monthly',
    'month_start',
    'ticker',
    2,
    chunk_time_interval => INTERVAL '1 year
);

-- Enable compression for data older than 1 year
ALTER TABLE stock_price_monthly SET (timescaledb.compress, timescaledb.compress_segmentby = 'ticker');
SELECT add_compression_policy('stock_price_monthly', INTERVAL '1 year');

-- 2. Monthly Indicators Table (Hypertable)
CREATE TABLE stock_indicators_monthly (
    ticker        VARCHAR(16) NOT NULL REFERENCES stocks(ticker),
    month_start   DATE        NOT NULL,
    ema_3         DOUBLE PRECISION,  -- 3-month EMA (≈50-day EMA)
    ema_10        DOUBLE PRECISION,  -- 10-month EMA (≈200-day EMA)
    rsi           DOUBLE PRECISION,
    atr           DOUBLE PRECISION,
    supertrend    DOUBLE PRECISION,
    obv           DOUBLE PRECISION,
    ad            DOUBLE PRECISION,
    volume_surge  DOUBLE PRECISION,
    PRIMARY KEY (ticker, month_start)
);

-- Convert to hypertable with 3-month chunk interval
SELECT create_hypertable(
    'stock_indicators_monthly',
    'month_start',
    'ticker',
    2,
    chunk_time_interval => INTERVAL '1 year
);

-- Enable compression for data older than 1 year
ALTER TABLE stock_indicators_monthly SET (timescaledb.compress, timescaledb.compress_segmentby = 'ticker');
SELECT add_compression_policy('stock_indicators_monthly', INTERVAL '1 year');



—--------------------------------------------------------------

-- Convert to hypertable (run once):
SELECT create_hypertable('fact_price', 'trade_date', chunk_time_interval => INTERVAL '1 month');

-- 5. Continuous Aggregate: Weekly Prices
CREATE MATERIALIZED VIEW fact_price_weekly
WITH (timescaledb.continuous) AS
SELECT
    stock_id,
    time_bucket('1 week', trade_date) as week_start_date,
    first(open_price, trade_date)       as open_price,
    max(high_price)                     as high_price,
    min(low_price)                      as low_price,
    last(close_price, trade_date)       as close_price,
    sum(volume)                         as volume
FROM fact_price
GROUP BY stock_id, time_bucket('1 week', trade_date);

-- 6. Fact: Technical Indicators
CREATE TABLE fact_technical (
    stock_id          INTEGER      NOT NULL REFERENCES dim_stock(stock_id),
    data_date         DATE         NOT NULL,
    frequency         VARCHAR(10)  NOT NULL CHECK (frequency IN ('daily','weekly','monthly')),
    indicator_name    VARCHAR(32)  NOT NULL,
    indicator_value   NUMERIC(18,6) NOT NULL,
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (stock_id, data_date, frequency, indicator_name)
);
-- Convert to hypertable:
SELECT create_hypertable('fact_technical', 'data_date', chunk_time_interval => INTERVAL '1 month');

-- 7. Fact: Relative Strength
CREATE TABLE fact_relative_strength (
    stock_id       INTEGER      NOT NULL REFERENCES dim_stock(stock_id),
    data_date      DATE         NOT NULL,
    frequency      VARCHAR(10)  NOT NULL CHECK (frequency IN ('daily','weekly','monthly')),
    rs_nifty50     NUMERIC(18,6) NOT NULL,
    rs_sector      NUMERIC(18,6) NOT NULL,
    rs_industry    NUMERIC(18,6) NOT NULL,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (stock_id, data_date, frequency)
);
-- Convert to hypertable:
SELECT create_hypertable('fact_relative_strength', 'data_date', chunk_time_interval => INTERVAL '1 month');

-- 8. Fact: Fundamentals Raw
CREATE TABLE fact_fundamentals_raw (
    stock_id             INTEGER      NOT NULL REFERENCES dim_stock(stock_id),
    quarter_end_date     DATE         NOT NULL,
    eps                  NUMERIC(18,4),
    revenue              NUMERIC(18,2),
    gross_profit         NUMERIC(18,2),
    shares_outstanding   BIGINT,
    ebit                 NUMERIC(18,2),
    equity               NUMERIC(18,2),
    total_assets         NUMERIC(18,2),
    total_liabilities    NUMERIC(18,2),
    cost_of_goods_sold   NUMERIC(18,2),
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (stock_id, quarter_end_date)
);

-- 9. Fact: Fundamental Metrics
CREATE TABLE fact_fundamental_metrics (
    stock_id            INTEGER      NOT NULL REFERENCES dim_stock(stock_id),
    quarter_end_date    DATE         NOT NULL,
    eps_qoq_pct         NUMERIC(8,4),
    eps_yoy_pct         NUMERIC(8,4),
    revenue_qoq_pct     NUMERIC(8,4),
    revenue_yoy_pct     NUMERIC(8,4),
    gross_margin_pct    NUMERIC(8,4),
    gm_qoq_pct          NUMERIC(8,4),
    gm_yoy_pct          NUMERIC(8,4),
    roe_pct             NUMERIC(8,4),
    roce_pct            NUMERIC(8,4),
    roa_pct             NUMERIC(8,4),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (stock_id, quarter_end_date)
);

-- Master table for the Stock Market Data store. This table contains list of all stocks which is important for the system.
CREATE TABLE stocks (
    ticker            VARCHAR(16)      PRIMARY KEY,
    company_name      TEXT,
    listing_date      DATE,
    sector_name       VARCHAR(32),      
    industry_name     VARCHAR(32),
    is_active         BOOLEAN          NOT NULL DEFAULT TRUE,
    last_price_fetch  DATE,
    last_fund_fetch   DATE,
    created_at        TIMESTAMPTZ      NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ      NOT NULL DEFAULT now()
);
CREATE INDEX idx_dim_stock_sector       ON stocks(sector_name);
CREATE INDEX idx_dim_stock_industry     ON stocks(industry_name);
CREATE INDEX idx_dim_stock_is_active    ON stocks(is_active);


-- 2. Price Daily: Stock_Price_Daily (Hypertable)
CREATE TABLE stock_price_daily (
    ticker      VARCHAR(16) NOT NULL REFERENCES stocks(ticker),
    date        DATE          NOT NULL,
    open        NUMERIC,
    high        NUMERIC,
    low         NUMERIC,
    close       NUMERIC,
    volume      BIGINT,
    adjusted    NUMERIC,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (ticker, date)
);
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


CREATE EXTENSION IF NOT EXISTS timescaledb;
SELECT create_hypertable('stock_prices', 'date', 'symbol', 2, if_not_exists => TRUE);
CREATE TABLE IF NOT EXISTS kind_paid_in_capital (
    id                      SERIAL PRIMARY KEY,
    reference_date          DATE         NOT NULL,
    company_name            VARCHAR(200) NOT NULL,
    increase_type           VARCHAR(100),
    stock_type              VARCHAR(100),
    issued_shares           BIGINT,
    allotment_ratio         NUMERIC(20, 6),
    employee_subscribe_date DATE,
    existing_holder_date    DATE,
    payment_date            DATE,
    original_link           TEXT,
    crawled_at              TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    CONSTRAINT uq_kind_paid_in_capital
        UNIQUE (reference_date, company_name, payment_date)
);
CREATE INDEX ... ON reference_date, company_name
import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation

import psycopg2
from psycopg2.extras import execute_values

from config import DB_CONFIG

logger = logging.getLogger(__name__)

CREATE_CRAWLED_MONTHS_SQL = """
CREATE TABLE IF NOT EXISTS crawled_months (
    year         INT NOT NULL,
    month        INT NOT NULL,
    record_count INT NOT NULL DEFAULT 0,
    PRIMARY KEY (year, month)
);
"""

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS kind_paid_in_capital (
    id                      SERIAL PRIMARY KEY,
    reference_date          DATE         NOT NULL,
    stock_code              VARCHAR(10),
    company_name            VARCHAR(200) NOT NULL,
    increase_type           VARCHAR(100),
    stock_type              VARCHAR(100),
    issued_shares           BIGINT,
    allotment_ratio         NUMERIC(20, 6),
    employee_subscribe_date DATE,
    existing_holder_date    DATE,
    payment_date            DATE,
    crawled_at              TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    CONSTRAINT uq_kind_paid_in_capital
        UNIQUE (reference_date, company_name, payment_date)
);

ALTER TABLE kind_paid_in_capital
    ADD COLUMN IF NOT EXISTS stock_code VARCHAR(10);

CREATE INDEX IF NOT EXISTS idx_kind_pic_reference_date
    ON kind_paid_in_capital (reference_date);

CREATE INDEX IF NOT EXISTS idx_kind_pic_company_name
    ON kind_paid_in_capital (company_name);

CREATE INDEX IF NOT EXISTS idx_kind_pic_stock_code
    ON kind_paid_in_capital (stock_code);
"""

INSERT_SQL = """
INSERT INTO kind_paid_in_capital (
    reference_date, stock_code, company_name, increase_type, stock_type,
    issued_shares, allotment_ratio,
    employee_subscribe_date, existing_holder_date, payment_date
) VALUES %s
ON CONFLICT ON CONSTRAINT uq_kind_paid_in_capital DO NOTHING
"""


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def initialize_db():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_CRAWLED_MONTHS_SQL)
            cur.execute(CREATE_TABLE_SQL)
        conn.commit()
    logger.info("DB 초기화 완료 (kind_paid_in_capital, crawled_months)")


def _parse_date(date_str: str):
    if not date_str or date_str.strip() in ("-", "", "N/A"):
        return None
    try:
        return datetime.strptime(date_str.strip(), "%Y-%m-%d").date()
    except ValueError:
        return None


def _parse_bigint(num_str: str):
    if not num_str or num_str.strip() in ("-", "", "N/A"):
        return None
    try:
        return int(num_str.replace(",", "").strip())
    except ValueError:
        return None


def _parse_numeric(num_str: str):
    if not num_str or num_str.strip() in ("-", "", "N/A"):
        return None
    try:
        return Decimal(num_str.replace(",", "").strip())
    except InvalidOperation:
        return None


def save_records_to_db(conn, records: list[dict]) -> tuple[int, int]:
    """레코드를 DB에 저장한다. 중복은 ON CONFLICT DO NOTHING으로 건너뜀."""
    if not records:
        return 0, 0

    rows = [
        (
            _parse_date(r["기준일"]),
            r.get("종목코드") or None,
            r["회사명"],
            r["증자구분"] or None,
            r["주식의종류"] or None,
            _parse_bigint(r["발행주식수"]),
            _parse_numeric(r["주당신주배정주식수"]),
            _parse_date(r["우리사주청약일"]),
            _parse_date(r["구주주청약일"]),
            _parse_date(r["납입일"]),
        )
        for r in records
    ]

    with conn.cursor() as cur:
        execute_values(cur, INSERT_SQL, rows)
        inserted = cur.rowcount
    conn.commit()

    skipped = len(records) - inserted
    return inserted, skipped


def mark_month_crawled(conn, year: int, month: int, record_count: int) -> None:
    """crawled_months 테이블에 수집 완료 월을 기록한다. 0건이어도 기록."""
    sql = """
    INSERT INTO crawled_months (year, month, record_count)
    VALUES (%s, %s, %s)
    ON CONFLICT (year, month) DO UPDATE
        SET record_count = EXCLUDED.record_count
    """
    with conn.cursor() as cur:
        cur.execute(sql, (year, month, record_count))
    conn.commit()


def get_already_crawled_months(conn) -> set[tuple[int, int]]:
    """crawled_months 테이블에서 이미 처리한 (year, month) 집합을 반환한다.
    데이터가 0건인 월도 포함된다."""
    sql = "SELECT year, month FROM crawled_months"
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    return {(int(r[0]), int(r[1])) for r in rows}

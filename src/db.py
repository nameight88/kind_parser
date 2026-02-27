import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation

import psycopg2
from psycopg2.extras import execute_values

from config import DB_CONFIG

logger = logging.getLogger(__name__)

CREATE_TABLE_SQL = """
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

CREATE INDEX IF NOT EXISTS idx_kind_pic_reference_date
    ON kind_paid_in_capital (reference_date);

CREATE INDEX IF NOT EXISTS idx_kind_pic_company_name
    ON kind_paid_in_capital (company_name);
"""

INSERT_SQL = """
INSERT INTO kind_paid_in_capital (
    reference_date, company_name, increase_type, stock_type,
    issued_shares, allotment_ratio,
    employee_subscribe_date, existing_holder_date, payment_date,
    original_link
) VALUES %s
ON CONFLICT ON CONSTRAINT uq_kind_paid_in_capital DO NOTHING
"""


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def initialize_db():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
        conn.commit()
    logger.info("DB 초기화 완료 (kind_paid_in_capital)")


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
            r["회사명"],
            r["증자구분"] or None,
            r["주식의종류"] or None,
            _parse_bigint(r["발행주식수"]),
            _parse_numeric(r["주당신주배정주식수"]),
            _parse_date(r["우리사주청약일"]),
            _parse_date(r["구주주청약일"]),
            _parse_date(r["납입일"]),
            r["원문링크"] or None,
        )
        for r in records
    ]

    with conn.cursor() as cur:
        execute_values(cur, INSERT_SQL, rows)
        inserted = cur.rowcount
    conn.commit()

    skipped = len(records) - inserted
    return inserted, skipped


def get_already_crawled_months(conn) -> set[tuple[int, int]]:
    """DB에 이미 저장된 (year, month) 집합을 반환한다."""
    sql = """
    SELECT DISTINCT
        EXTRACT(YEAR  FROM reference_date)::INT,
        EXTRACT(MONTH FROM reference_date)::INT
    FROM kind_paid_in_capital
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    return {(int(r[0]), int(r[1])) for r in rows}

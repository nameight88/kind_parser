import sys
import time
import logging
import os

from dotenv import load_dotenv

# .env 파일 로드 (DB 접속 정보)
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

from config import (
    START_YEAR, START_MONTH, END_YEAR, END_MONTH,
    LOG_PATH, DATA_DIR,
)
from scraper import create_session, fetch_month_data
from db import get_connection, initialize_db, save_records_to_db, get_already_crawled_months, mark_month_crawled
from csv_writer import save_records_to_csv

# 로깅 설정
os.makedirs(DATA_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


def generate_month_range(
    start_year: int, start_month: int,
    end_year: int, end_month: int,
) -> list[tuple[int, int]]:
    """수집 대상 (year, month) 튜플 리스트를 생성한다."""
    months = []
    year, month = start_year, start_month
    while (year, month) <= (end_year, end_month):
        months.append((year, month))
        month += 1
        if month > 12:
            month = 1
            year += 1
    return months


def main():
    logger.info("=" * 60)
    logger.info("KIND 유상증자 크롤러 시작")
    logger.info(f"수집 기간: {START_YEAR}-{START_MONTH:02d} ~ {END_YEAR}-{END_MONTH:02d}")
    logger.info("=" * 60)

    # DB 연결 시도
    db_conn = None
    already_crawled: set[tuple[int, int]] = set()

    try:
        db_conn = get_connection()
        initialize_db()
        already_crawled = get_already_crawled_months(db_conn)
        logger.info(f"기존 수집 완료 월: {len(already_crawled)}개월")
    except Exception as e:
        logger.warning(f"DB 연결 실패: {e}")
        logger.info("CSV 전용 모드로 진행합니다.")

    # 수집 대상 월 결정
    all_months = generate_month_range(START_YEAR, START_MONTH, END_YEAR, END_MONTH)
    target_months = [(y, m) for y, m in all_months if (y, m) not in already_crawled]

    logger.info(f"신규 수집 대상: {len(target_months)}개월 / 전체 {len(all_months)}개월")

    if not target_months:
        logger.info("모든 데이터가 이미 수집되었습니다.")
        if db_conn:
            db_conn.close()
        return

    # HTTP 세션 생성
    session = create_session()

    all_new_records = []
    total_inserted = 0
    total_skipped = 0

    for idx, (year, month) in enumerate(target_months, start=1):
        logger.info(f"[{idx}/{len(target_months)}] {year}년 {month}월 수집 중...")

        records = fetch_month_data(session, year, month)
        db_save_ok = True  # DB 저장 성공 여부 (0건이면 저장 없이 성공으로 간주)

        if records:
            all_new_records.extend(records)

            if db_conn:
                try:
                    inserted, skipped = save_records_to_db(db_conn, records)
                    total_inserted += inserted
                    total_skipped += skipped
                    logger.info(f"  -> DB 저장: {inserted}건 신규 / {skipped}건 중복")
                except Exception as e:
                    logger.error(f"  -> DB 저장 실패: {e}")
                    db_save_ok = False

        # DB 저장 성공 시에만 수집 완료로 기록 (실패 시 다음 실행에서 재시도)
        if db_conn and db_save_ok:
            try:
                mark_month_crawled(db_conn, year, month, len(records))
            except Exception as e:
                logger.error(f"  -> 수집 완료 월 기록 실패: {e}")

        time.sleep(0.5)

    # CSV 일괄 저장
    try:
        save_records_to_csv(all_new_records)
    except Exception as e:
        logger.error(f"CSV 저장 실패: {e}")

    # 최종 요약
    logger.info("=" * 60)
    logger.info("크롤링 완료")
    logger.info(f"  처리 월수:      {len(target_months)}개월")
    logger.info(f"  수집 레코드:    {len(all_new_records)}건")
    if db_conn:
        logger.info(f"  DB 신규 저장:   {total_inserted}건")
        logger.info(f"  DB 중복 건너뜀: {total_skipped}건")
    logger.info("=" * 60)

    if db_conn:
        db_conn.close()


if __name__ == "__main__":
    main()

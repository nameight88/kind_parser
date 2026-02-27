import os
import logging

import pandas as pd

from config import CSV_OUTPUT_PATH, DATA_DIR

logger = logging.getLogger(__name__)

COLUMNS = [
    "기준일", "회사명", "증자구분", "주식의종류",
    "발행주식수", "주당신주배정주식수",
    "우리사주청약일", "구주주청약일", "납입일", "원문링크",
]


def save_records_to_csv(records: list[dict]) -> None:
    """
    레코드를 CSV로 저장한다.
    파일이 이미 존재하면 신규 데이터를 추가 후 중복을 제거한다.
    """
    if not records:
        logger.info("CSV에 저장할 레코드가 없습니다.")
        return

    os.makedirs(DATA_DIR, exist_ok=True)

    new_df = pd.DataFrame(records, columns=COLUMNS)

    if os.path.exists(CSV_OUTPUT_PATH):
        existing_df = pd.read_csv(CSV_OUTPUT_PATH, encoding="utf-8-sig", dtype=str)
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        combined_df.drop_duplicates(subset=["기준일", "회사명", "납입일"], keep="last", inplace=True)
        combined_df.sort_values("기준일", inplace=True)
        combined_df.to_csv(CSV_OUTPUT_PATH, index=False, encoding="utf-8-sig")
        logger.info(f"CSV 업데이트: 총 {len(combined_df)}건 ({CSV_OUTPUT_PATH})")
    else:
        new_df.sort_values("기준일", inplace=True)
        new_df.to_csv(CSV_OUTPUT_PATH, index=False, encoding="utf-8-sig")
        logger.info(f"CSV 신규 생성: {len(new_df)}건 ({CSV_OUTPUT_PATH})")

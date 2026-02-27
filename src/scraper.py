import time
import logging

import requests
from bs4 import BeautifulSoup

from config import (
    BASE_URL,
    METHOD,
    SEARCH_MENU,
    PERIOD_TYPE,
    HEADERS,
    MAX_RETRIES,
    RETRY_DELAY_SECONDS,
    REQUEST_TIMEOUT,
)

logger = logging.getLogger(__name__)


def create_session() -> requests.Session:
    """requests.Session 생성 후 KIND 메인 페이지 방문으로 쿠키를 획득한다."""
    session = requests.Session()
    session.headers.update(HEADERS)
    try:
        session.get(
            "https://kind.krx.co.kr/common/stockschedule.do?method=StockScheduleMain&index=3",
            timeout=REQUEST_TIMEOUT,
        )
    except Exception as e:
        logger.warning(f"초기 세션 요청 실패 (무시하고 진행): {e}")
    return session


def build_payload(year: int, month: int) -> dict:
    """월별 유상증자 조회를 위한 POST 파라미터를 구성한다.

    개발자 도구 네트워크 탭에서 확인한 실제 페이로드 구조를 사용한다.
    """
    import calendar

    # 해당 월의 마지막 날 계산 (selDate에 사용)
    last_day = calendar.monthrange(year, month)[1]
    return {
        "method": METHOD,
        "forward": "searchpaidincapitalincrease",
        "searchCodeType": "",
        "repIsuSrtCd": "",
        "menuIndex": "",
        "selMonth": f"{month:02d}",
        "selDay": "",
        "enterFlag": "",
        "searchMenu": SEARCH_MENU,
        "nowYear": str(year),
        "nowMonth": str(month),
        "searchCorpName": "회사명/코드",
        "selYear": str(year),
        "showMonth": str(month),
        "periodType": PERIOD_TYPE,
        "selDate": f"{year}-{month:02d}-{last_day:02d}",
        "ldMktTpCd": "",
        "submitFlagYn": "N",
    }


def fetch_month_data(session: requests.Session, year: int, month: int) -> list[dict]:
    """
    특정 연/월의 유상증자 데이터를 POST 요청으로 수집한다.
    최대 MAX_RETRIES회 재시도하며, 실패 시 빈 리스트를 반환한다.
    """
    payload = build_payload(year, month)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.post(
                BASE_URL,
                data=payload,
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            response.encoding = "utf-8"

            records = parse_html_table(response.text, year, month)
            logger.info(f"[{year}-{month:02d}] {len(records)}건 수집")
            return records

        except requests.exceptions.Timeout:
            logger.warning(
                f"[{year}-{month:02d}] 타임아웃 (시도 {attempt}/{MAX_RETRIES})"
            )
        except requests.exceptions.ConnectionError:
            logger.warning(
                f"[{year}-{month:02d}] 연결 오류 (시도 {attempt}/{MAX_RETRIES})"
            )
        except requests.exceptions.HTTPError as e:
            logger.error(
                f"[{year}-{month:02d}] HTTP 오류: {e} (시도 {attempt}/{MAX_RETRIES})"
            )
        except Exception as e:
            logger.error(
                f"[{year}-{month:02d}] 예외: {e} (시도 {attempt}/{MAX_RETRIES})"
            )

        if attempt < MAX_RETRIES:
            time.sleep(RETRY_DELAY_SECONDS * attempt)

    logger.error(f"[{year}-{month:02d}] 최대 재시도 초과. 건너뜀.")
    return []


def parse_html_table(html: str, year: int, month: int) -> list[dict]:
    """
    HTML 응답에서 유상증자 테이블 행을 파싱하여 딕셔너리 리스트로 반환한다.
    데이터가 없거나 tbody를 찾을 수 없으면 빈 리스트를 반환한다.
    """
    soup = BeautifulSoup(html, "html.parser")

    if soup.find(string=lambda t: t and "조회된 내역이 없습니다" in t):
        return []

    tbody = soup.find("tbody")
    if not tbody:
        logger.warning(f"[{year}-{month:02d}] tbody를 찾을 수 없음")
        return []

    records = []
    for tr in tbody.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 10:
            continue

        link_tag = tds[9].find("a")
        original_link = ""
        if link_tag and link_tag.get("href"):
            href = link_tag["href"].strip()
            original_link = (
                href if href.startswith("http") else f"https://kind.krx.co.kr{href}"
            )

        records.append(
            {
                "기준일": tds[0].get_text(strip=True),
                "회사명": tds[1].get_text(strip=True),
                "증자구분": tds[2].get_text(strip=True),
                "주식의종류": tds[3].get_text(strip=True),
                "발행주식수": tds[4].get_text(strip=True).replace(",", ""),
                "주당신주배정주식수": tds[5].get_text(strip=True).replace(",", ""),
                "우리사주청약일": tds[6].get_text(strip=True),
                "구주주청약일": tds[7].get_text(strip=True),
                "납입일": tds[8].get_text(strip=True),
                "원문링크": original_link,
            }
        )

    return records

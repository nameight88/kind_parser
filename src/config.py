import os
from datetime import datetime

# 수집 기간
START_YEAR = 2014
START_MONTH = 1
END_YEAR = datetime.now().year
END_MONTH = datetime.now().month

# KIND API 설정
BASE_URL = "https://kind.krx.co.kr/common/stockschedule.do"
METHOD = "searchPaidinCapitalIncrease"
SEARCH_MENU = "02"
PERIOD_TYPE = "M"

# HTTP 헤더 (개발자 도구에서 확인한 실제 요청 헤더)
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    ),
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Accept": "text/html, */*; q=0.01",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": "https://kind.krx.co.kr/common/stockschedule.do?method=StockScheduleMain&index=3",
    "Origin": "https://kind.krx.co.kr",
}

# 재시도 설정
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 2
REQUEST_TIMEOUT = 30

# 경로 설정
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
CSV_OUTPUT_PATH = os.path.join(DATA_DIR, "kind_paid_in_capital.csv")
LOG_PATH = os.path.join(DATA_DIR, "crawler.log")

# PostgreSQL 설정
DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "kind_db"),
    "user":     os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

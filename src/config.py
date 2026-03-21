"""
config.py — Barcha sozlamalar bir joyda.
.env fayldan o'qiydi; ishga tushirishda faqat shu faylni o'zgartirish kifoya.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# ── API ───────────────────────────────────────────────────────────────────────
SEARCH_TEXT    = os.getenv("SEARCH_TEXT", "data analyst")
AREA_ID        = os.getenv("AREA_ID", "97")        # 97 = Uzbekistan
PER_PAGE       = int(os.getenv("PER_PAGE", "100"))
TEST_MODE      = os.getenv("TEST_MODE", "false").lower() == "true"
MAX_PAGES_TEST = int(os.getenv("MAX_PAGES_TEST", "3"))
REQUEST_DELAY  = float(os.getenv("REQUEST_DELAY", "0.2"))

HEADERS = {
    "User-Agent": os.getenv("USER_AGENT", "hh-uz-collector/2.0 (your@email.com)")
}

BASE_LIST_URL   = "https://api.hh.ru/vacancies"
BASE_DETAIL_URL = "https://api.hh.ru/vacancies/{}"

# ── Database (SQL Server) ─────────────────────────────────────────────────────
DB_SERVER  = os.getenv("DB_SERVER", "DESKTOP-8V34E53")
DB_NAME    = os.getenv("DB_NAME", "headhunter")
DB_DRIVER  = os.getenv("DB_DRIVER", "ODBC Driver 18 for SQL Server")
DB_TRUSTED = os.getenv("DB_TRUSTED", "yes")
DB_CERT    = os.getenv("DB_CERT", "yes")

# ── Output ────────────────────────────────────────────────────────────────────
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "output")
CSV_PREFIX = "hh_"
LOG_LEVEL  = os.getenv("LOG_LEVEL", "INFO")

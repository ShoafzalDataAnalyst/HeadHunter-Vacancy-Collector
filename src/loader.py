"""
loader.py — DataFrame larni SQL Server ga yozadi va CSV eksport qiladi.

Mas'uliyat:
  - SQLAlchemy engine yaratish
  - Upsert (INSERT or SKIP by h_id) mantiq
  - CSV saqlash
  - Power BI uchun SQL View yaratish
"""

import os
import logging
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text

import config

log = logging.getLogger(__name__)


# ── Engine ─────────────────────────────────────────────────────────────────────

def build_engine():
    """SQL Server uchun SQLAlchemy engine qaytaradi (Windows auth)."""
    conn_str = (
        f"DRIVER={{{config.DB_DRIVER}}};"
        f"SERVER={config.DB_SERVER};"
        f"DATABASE={config.DB_NAME};"
        f"Trusted_Connection={config.DB_TRUSTED};"
        f"TrustServerCertificate={config.DB_CERT};"
    )
    url = f"mssql+pyodbc:///?odbc_connect={quote_plus(conn_str)}"
    return create_engine(url, fast_executemany=True)   # fast_executemany tezlikni oshiradi


# ── CSV eksport ────────────────────────────────────────────────────────────────

def save_csv(df: pd.DataFrame, name: str) -> str:
    """DataFrame ni output/ papkasiga saqlaydi, yo'lini qaytaradi."""
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    path = os.path.join(config.OUTPUT_DIR, f"{config.CSV_PREFIX}{name}.csv")
    df.to_csv(path, index=False, encoding="utf-8-sig")   # utf-8-sig Excel uchun
    log.info("CSV saqlandi: %s (%d qator)", path, len(df))
    return path


# ── DB yuklash ─────────────────────────────────────────────────────────────────

def upsert_table(df: pd.DataFrame, table: str, pk: str, engine) -> int:
    """
    Mavjud pk larni o'tkazib yuboradi, yangi qatorlarni qo'shadi.
    SQL Server da MERGE yoki temp-table pattern ishlatiladi.
    """
    if df.empty:
        return 0

    with engine.connect() as conn:
        # Bazadagi mavjud pk larni olamiz
        existing = pd.read_sql(f"SELECT [{pk}] FROM [{table}]", conn)
        existing_set = set(existing[pk].astype(str))

    new_rows = df[~df[pk].astype(str).isin(existing_set)]
    if new_rows.empty:
        log.info("%s: yangi qator yo'q (hammasi mavjud)", table)
        return 0

    new_rows.to_sql(table, con=engine, if_exists="append", index=False)
    log.info("%s: %d yangi qator qo'shildi", table, len(new_rows))
    return len(new_rows)


def load_all(
    df_vacancies:     pd.DataFrame,
    df_companies:     pd.DataFrame,
    df_locations:     pd.DataFrame,
    df_skills:        pd.DataFrame,
    df_vacancy_skill: pd.DataFrame,
    engine,
) -> dict:
    """Barcha jadvallarni tartib bilan yuklaydi. Statistika dict qaytaradi."""
    stats = {}
    # Referans jadvallar avval (FK tartib)
    stats["companies"]     = upsert_table(df_companies,     "companies",     "id",     engine)
    stats["locations"]     = upsert_table(df_locations,     "locations",     "id",     engine)
    stats["skills"]        = upsert_table(df_skills,        "skills",        "id",     engine)
    stats["vacancies"]     = upsert_table(df_vacancies,     "vacancies",     "h_id",   engine)
    # vacancy_skill uchun (h_id, skill_id) juft kalit
    stats["vacancy_skill"] = _upsert_vacancy_skill(df_vacancy_skill, engine)
    return stats


def _upsert_vacancy_skill(df: pd.DataFrame, engine) -> int:
    """vacancy_skill M:N jadvalida dublikatlarni oldini oladi."""
    if df.empty:
        return 0
    with engine.connect() as conn:
        existing = pd.read_sql("SELECT h_id, skill_id FROM vacancy_skill", conn)
    existing["_key"] = existing["h_id"].astype(str) + "_" + existing["skill_id"].astype(str)
    df["_key"]       = df["h_id"].astype(str)       + "_" + df["skill_id"].astype(str)
    new_rows = df[~df["_key"].isin(existing["_key"])].drop(columns=["_key"])
    if new_rows.empty:
        return 0
    new_rows.to_sql("vacancy_skill", con=engine, if_exists="append", index=False)
    log.info("vacancy_skill: %d yangi bog'lanish qo'shildi", len(new_rows))
    return len(new_rows)


# ── Power BI SQL View lar ──────────────────────────────────────────────────────

POWERBI_VIEWS = {

    "vw_vacancies_full": """
        CREATE OR ALTER VIEW vw_vacancies_full AS
        SELECT
            v.h_id,
            v.title,
            v.position,
            v.category,
            v.publish_date,
            v.company,
            v.country,
            v.location,
            v.min_salary,
            v.max_salary,
            v.currency,
            -- USD ga konvertatsiya (taxminiy kurs, kerak bo'lsa dinamik qiling)
            CASE v.currency
                WHEN 'UZS' THEN v.min_salary / 12500.0
                WHEN 'USD' THEN v.min_salary
                WHEN 'EUR' THEN v.min_salary * 1.08
                ELSE v.min_salary
            END AS min_salary_usd,
            CASE v.currency
                WHEN 'UZS' THEN v.max_salary / 12500.0
                WHEN 'USD' THEN v.max_salary
                WHEN 'EUR' THEN v.max_salary * 1.08
                ELSE v.max_salary
            END AS max_salary_usd,
            v.skills
        FROM vacancies v
    """,

    "vw_skill_demand": """
        CREATE OR ALTER VIEW vw_skill_demand AS
        SELECT
            s.name        AS skill_name,
            COUNT(vs.h_id) AS vacancy_count,
            -- nechta kompaniya bu skillni talab qiladi
            COUNT(DISTINCT v.company) AS company_count
        FROM skills s
        JOIN vacancy_skill vs ON vs.skill_id = s.id
        JOIN vacancies v      ON v.h_id = vs.h_id
        GROUP BY s.name
    """,

    "vw_salary_by_category": """
        CREATE OR ALTER VIEW vw_salary_by_category AS
        SELECT
            category,
            COUNT(*)                         AS vacancy_count,
            AVG(CASE currency WHEN 'UZS' THEN min_salary/12500.0
                              WHEN 'USD' THEN min_salary
                              WHEN 'EUR' THEN min_salary*1.08
                              ELSE min_salary END) AS avg_min_usd,
            AVG(CASE currency WHEN 'UZS' THEN max_salary/12500.0
                              WHEN 'USD' THEN max_salary
                              WHEN 'EUR' THEN max_salary*1.08
                              ELSE max_salary END) AS avg_max_usd,
            MIN(CASE currency WHEN 'UZS' THEN min_salary/12500.0
                              WHEN 'USD' THEN min_salary
                              ELSE min_salary END) AS floor_usd,
            MAX(CASE currency WHEN 'UZS' THEN max_salary/12500.0
                              WHEN 'USD' THEN max_salary
                              ELSE max_salary END) AS ceiling_usd
        FROM vacancies
        WHERE category IS NOT NULL
        GROUP BY category
    """,

    "vw_daily_posting_trend": """
        CREATE OR ALTER VIEW vw_daily_posting_trend AS
        SELECT
            publish_date,
            COUNT(*)                      AS vacancies_posted,
            COUNT(DISTINCT company)       AS unique_companies,
            SUM(COUNT(*)) OVER (ORDER BY publish_date ROWS UNBOUNDED PRECEDING)
                                          AS cumulative_total
        FROM vacancies
        GROUP BY publish_date
    """,

    "vw_top_hiring_companies": """
        CREATE OR ALTER VIEW vw_top_hiring_companies AS
        SELECT
            v.company,
            c.website,
            COUNT(v.h_id)                 AS open_positions,
            MIN(v.publish_date)           AS first_posted,
            MAX(v.publish_date)           AS last_posted,
            AVG(CASE v.currency
                    WHEN 'USD' THEN v.max_salary
                    WHEN 'UZS' THEN v.max_salary / 12500.0
                    ELSE NULL END)        AS avg_max_salary_usd
        FROM vacancies v
        LEFT JOIN companies c ON c.name = v.company
        GROUP BY v.company, c.website
    """,

    "vw_location_heatmap": """
        CREATE OR ALTER VIEW vw_location_heatmap AS
        SELECT
            country,
            location   AS city,
            COUNT(*)   AS vacancy_count,
            COUNT(DISTINCT company) AS company_count,
            AVG(CASE currency WHEN 'USD' THEN min_salary
                              WHEN 'UZS' THEN min_salary/12500.0
                              ELSE NULL END) AS avg_min_usd
        FROM vacancies
        GROUP BY country, location
    """,
}


def create_powerbi_views(engine) -> None:
    """Power BI uchun barcha View larni SQL Server da yaratadi."""
    with engine.connect() as conn:
        for view_name, ddl in POWERBI_VIEWS.items():
            try:
                conn.execute(text(ddl))
                conn.commit()
                log.info("View yaratildi: %s", view_name)
            except Exception as exc:
                log.error("View xatosi (%s): %s", view_name, exc)

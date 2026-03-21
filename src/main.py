"""
main.py — ETL Pipeline orchestrator.

Ishga tushirish:
    python src/main.py

Muhit o'zgaruvchilari (.env):
    TEST_MODE=true   → faqat 3 sahifa (tezkor test)
    TEST_MODE=false  → to'liq yig'ish
"""

import logging
import sys
import os

import pandas as pd

import config
import collector
import cleaner
import loader

# ── Logging sozlamasi ──────────────────────────────────────────────────────────
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL, logging.INFO),
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("etl.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("main")


def run():
    log.info("=" * 60)
    log.info("HeadHunter ETL boshlandi")
    log.info("Qidiruv: '%s' | Hudud: %s | TEST_MODE: %s",
             config.SEARCH_TEXT, config.AREA_ID, config.TEST_MODE)
    log.info("=" * 60)

    # ── 1. Area ID ─────────────────────────────────────────────────────────────
    area_id = config.AREA_ID
    if not area_id:
        log.info("AREA_ID topilmadi, API dan qidirilmoqda...")
        area_id = collector.find_area_id()
        if not area_id:
            log.critical("Uzbekiston area_id topilmadi. Dastur to'xtadi.")
            sys.exit(1)
    log.info("Area ID: %s", area_id)

    # ── 2. Vacancy ID lar yig'ish ──────────────────────────────────────────────
    store = cleaner.NormalizationStore()
    vacancy_rows: list[dict] = []
    skill_links:  list[dict] = []
    seen_ids: set = set()

    for vac_id in collector.iter_vacancy_ids(area_id, config.SEARCH_TEXT):
        if vac_id in seen_ids:
            continue
        seen_ids.add(vac_id)

        detail = collector.fetch_vacancy_detail(vac_id)
        if not detail:
            log.warning("Detail yo'q: %s", vac_id)
            continue

        vac_row, links = cleaner.parse_vacancy(detail, store)
        vacancy_rows.append(vac_row)
        skill_links.extend(links)

    log.info("Jami %d noyob vacancy yig'ildi", len(vacancy_rows))

    if not vacancy_rows:
        log.warning("Hech qanday ma'lumot topilmadi.")
        return

    # ── 3. DataFrame lar tuzish ────────────────────────────────────────────────
    df_vac = (
        pd.DataFrame(vacancy_rows)
        .drop_duplicates(subset=["h_id"], keep="first")
        .reset_index(drop=True)
    )

    df_companies = pd.DataFrame.from_records(
        [{"id": v["id"], "name": v["name"], "website": v["website"]}
         for v in store.companies.values()]
    )
    df_locations = pd.DataFrame.from_records(
        [{"id": v["id"], "country": v["country"], "city": v["city"]}
         for v in store.locations.values()]
    )
    df_skills = pd.DataFrame.from_records(
        [{"id": v["id"], "name": v["name"]}
         for v in store.skills.values()]
    )
    df_vacancy_skill = pd.DataFrame(skill_links).drop_duplicates()

    # DB ga yoziladigan ustunlar (FK lar CSV da qoladi)
    df_vac_db = df_vac[[
        "h_id", "title", "position", "category", "publish_date",
        "company", "skills", "country", "location",
        "min_salary", "max_salary", "currency"
    ]]

    log.info("Vacancies: %d | Companies: %d | Locations: %d | Skills: %d | Links: %d",
             len(df_vac_db), len(df_companies), len(df_locations),
             len(df_skills), len(df_vacancy_skill))

    # ── 4. CSV saqlash ─────────────────────────────────────────────────────────
    loader.save_csv(df_vac_db,        "vacancies")
    loader.save_csv(df_companies,     "companies")
    loader.save_csv(df_locations,     "locations")
    loader.save_csv(df_skills,        "skills")
    loader.save_csv(df_vacancy_skill, "vacancy_skill")

    # ── 5. DB yuklash ──────────────────────────────────────────────────────────
    try:
        engine = loader.build_engine()
        stats  = loader.load_all(
            df_vac_db, df_companies, df_locations,
            df_skills, df_vacancy_skill, engine
        )
        log.info("DB yuklash natijasi: %s", stats)

        # ── 6. Power BI View lar ───────────────────────────────────────────────
        loader.create_powerbi_views(engine)
        log.info("Power BI View lar muvaffaqiyatli yaratildi.")

    except Exception as exc:
        log.error("DB ulanish xatosi: %s", exc)
        log.info("CSV fayllar saqlangan — DB dan mustaqil ishlatish mumkin.")

    log.info("ETL muvaffaqiyatli yakunlandi.")


if __name__ == "__main__":
    run()

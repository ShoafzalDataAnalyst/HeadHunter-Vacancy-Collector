"""
cleaner.py — Xom API JSON → toza Python dict.

Mas'uliyat:
  - Matn tozalash (whitespace, encoding)
  - Maosh (salary) parse
  - Skill normalizatsiya
  - Joylashuv (country / city) ajratish
  - Normalizatsiya map larini boshqarish (companies, locations, skills)

Bu modul requests yoki DB ga tegmaydi.
"""

import re
import logging
from dataclasses import dataclass, field
from typing import Optional

log = logging.getLogger(__name__)


# ── Yordamchi funksiyalar ──────────────────────────────────────────────────────

def clean_text(value) -> Optional[str]:
    """Ko'p bo'shliqlarni bitta bo'shliqqa kamaytiradi va trim qiladi."""
    if value is None:
        return None
    return re.sub(r"\s+", " ", str(value)).strip() or None


def normalize_skill(skill: str) -> Optional[str]:
    """Skill nomini kichik harf + trim + dash unifikatsiya."""
    if not skill:
        return None
    return skill.strip().lower().replace("–", "-").replace("—", "-")


def parse_salary(sal: dict | None) -> tuple[Optional[float], Optional[float], Optional[str]]:
    """(min, max, currency) qaytaradi; ma'lumot yo'q bo'lsa (None, None, None)."""
    if not sal:
        return None, None, None
    return sal.get("from"), sal.get("to"), sal.get("currency")


def split_area(area_name: str) -> tuple[str, str]:
    """
    HH area nomi odatda "Tashkent" yoki "Uzbekistan, Tashkent" shaklida keladi.
    (country, city) qaytaradi.
    """
    if not area_name:
        return "Uzbekistan", ""
    parts = [p.strip() for p in area_name.split(",")]
    if len(parts) == 1:
        # Faqat shahar nomi kelsa, mamlakat Uzbekistan deb qabul qilinadi
        return "Uzbekistan", parts[0]
    return parts[0], parts[1]


# ── Normalizatsiya holati ──────────────────────────────────────────────────────

@dataclass
class NormalizationStore:
    """
    companies, locations, skills mapping larini saqlaydi.
    ETL davomida yagona instance ishlatiladi.
    """
    companies: dict  = field(default_factory=dict)   # name  → {id, name, website}
    locations: dict  = field(default_factory=dict)   # key   → {id, country, city}
    skills:    dict  = field(default_factory=dict)   # norm  → {id, name}

    _company_ctr:  int = field(default=1, repr=False)
    _location_ctr: int = field(default=1, repr=False)
    _skill_ctr:    int = field(default=1, repr=False)

    def get_or_add_company(self, name: str, website: str | None) -> int | None:
        if not name:
            return None
        if name not in self.companies:
            self.companies[name] = {"id": self._company_ctr, "name": name, "website": website}
            self._company_ctr += 1
        return self.companies[name]["id"]

    def get_or_add_location(self, country: str, city: str) -> int:
        key = f"{country}|{city}"
        if key not in self.locations:
            self.locations[key] = {"id": self._location_ctr, "country": country, "city": city}
            self._location_ctr += 1
        return self.locations[key]["id"]

    def get_or_add_skill(self, raw_name: str) -> int | None:
        norm = normalize_skill(raw_name)
        if not norm:
            return None
        if norm not in self.skills:
            self.skills[norm] = {"id": self._skill_ctr, "name": norm}
            self._skill_ctr += 1
        return self.skills[norm]["id"]


# ── Asosiy parser ──────────────────────────────────────────────────────────────

def parse_vacancy(detail: dict, store: NormalizationStore) -> tuple[dict, list[dict]]:
    """
    Bitta vacancy JSON ni toza vacancy_row va [vacancy_skill] listga aylantiradi.

    Returns:
        vacancy_row  — vacancies jadvalining bir qatori
        skill_links  — [{"h_id": ..., "skill_id": ...}] (vacancy_skill jadvali uchun)
    """
    hid = detail.get("id")

    # ── Asosiy maydonlar ───────────────────────────────────────────────────────
    title    = clean_text(detail.get("name"))
    category = _extract_category(detail)
    published_at = detail.get("published_at") or ""
    publish_date = published_at[:10] if published_at else None

    # ── Kompaniya ──────────────────────────────────────────────────────────────
    employer     = detail.get("employer") or {}
    company_name = clean_text(employer.get("name"))
    company_site = employer.get("alternate_url")
    company_id   = store.get_or_add_company(company_name, company_site)

    # ── Joylashuv ──────────────────────────────────────────────────────────────
    area_name  = (detail.get("area") or {}).get("name") or ""
    country, city = split_area(area_name)
    location_id   = store.get_or_add_location(country, city)

    # ── Maosh ──────────────────────────────────────────────────────────────────
    min_sal, max_sal, currency = parse_salary(detail.get("salary"))

    # ── Skills ─────────────────────────────────────────────────────────────────
    raw_skills = [ks.get("name") for ks in (detail.get("key_skills") or []) if ks.get("name")]
    skill_ids  = [store.get_or_add_skill(s) for s in raw_skills]
    skill_ids  = [sid for sid in skill_ids if sid is not None]
    skill_names_norm = [normalize_skill(s) for s in raw_skills if normalize_skill(s)]

    vacancy_row = {
        "h_id":        hid,
        "title":       title,
        "position":    _infer_position(title),   # title dan ajratib olinadi
        "category":    category,
        "publish_date": publish_date,
        "company":     company_name,
        "company_id":  company_id,
        "country":     country,
        "location":    city,
        "location_id": location_id,
        "min_salary":  min_sal,
        "max_salary":  max_sal,
        "currency":    currency,
        "skills":      ";".join(skill_names_norm),   # CSV + Power BI uchun
    }

    skill_links = [{"h_id": hid, "skill_id": sid} for sid in skill_ids]
    return vacancy_row, skill_links


# ── Ichki yordamchilar ─────────────────────────────────────────────────────────

def _extract_category(detail: dict) -> Optional[str]:
    """HH 'specializations' yoki 'professional_roles' dan kategoriya oladi."""
    # Yangi API: professional_roles
    roles = detail.get("professional_roles") or []
    if roles:
        return clean_text(roles[0].get("name"))
    # Eski API: specializations
    specs = detail.get("specializations") or []
    if specs:
        sp = specs[0]
        return clean_text(sp.get("profarea_name") or sp.get("name"))
    return None


def _infer_position(title: str | None) -> Optional[str]:
    """
    Vacancy sarlavhasidan lavozimni aniqlaydi.
    Misol: "Junior Data Analyst (Tashkent)" → "Junior Data Analyst"
    Kelajakda ML classifier bilan almashtirilishi mumkin.
    """
    if not title:
        return None
    # Qavslar ichidagi qo'shimchalarni olib tashlaydi
    clean = re.sub(r"\(.*?\)|\[.*?\]", "", title).strip()
    return clean or title

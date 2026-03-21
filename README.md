# HeadHunter Vacancy Collector 🇺🇿

> A production-grade ETL pipeline that collects **Data Analyst** job vacancies from [hh.uz](https://hh.uz) via the official API, cleans and normalizes the data, loads it into SQL Server, and exposes ready-to-use **Power BI views** for market analysis.

---

## Dashboard Preview

![Dashboard](docs/dashboard_preview.png)

> **Key Insights (September 2025):**
> - 🏢 **71** active vacancies across **59** companies
> - 🛠️ **SQL, Python, Power BI** are the top 3 demanded skills
> - 💰 Only **12.68%** of companies disclose salary
> - 🏦 **Ipotekabank OTP Group, TBC, AVO.UZ** are top hirers

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                    src/main.py                       │
│              (ETL Orchestrator)                      │
└───────────┬─────────────────┬───────────────────────┘
            │                 │
     ┌──────▼──────┐   ┌──────▼──────┐
     │ collector.py│   │  cleaner.py │
     │  HH API     │   │  Transform  │
     │  + Retry    │   │  + Normalize│
     └──────┬──────┘   └──────┬──────┘
            └────────┬─────────┘
                     │
              ┌──────▼──────┐
              │  loader.py  │
              │  SQL Server │
              │  CSV Export │
              │  Power BI   │
              │  Views      │
              └─────────────┘
```

### Database Schema

```
companies ──────────────────────────┐
locations ──────────────────────────┤
                                    ▼
                               vacancies  ◄──── vacancy_skill ◄──── skills
```

---

## Project Structure

```
HeadHunter-Vacancy-Collector/
├── src/
│   ├── main.py          # ETL pipeline entry point
│   ├── collector.py     # HH API: list + detail fetching, retry logic
│   ├── cleaner.py       # Data cleaning, normalization, skill parsing
│   ├── loader.py        # SQL Server upsert, CSV export, Power BI views
│   └── config.py        # All settings via .env
├── sql/
│   └── schema.sql       # SQL Server table definitions (idempotent)
├── docs/
│   └── dashboard_preview.png   # Power BI dashboard screenshot
├── analysis.ipynb       # EDA, data cleaning, visualizations
├── hh_dashboard.pbix    # Power BI dashboard file
├── .env.example         # Environment variable template
├── requirements.txt
└── README.md
```

---

## Quick Start

### 1. Clone & setup
```bash
git clone https://github.com/ShoafzalDataAnalyst/HeadHunter-Vacancy-Collector.git
cd HeadHunter-Vacancy-Collector
python -m venv .venv
.venv\Scripts\activate        # Windows
pip install -r requirements.txt
```

### 2. Configure
```bash
cp .env.example .env
# .env faylini oching va DB_SERVER ni o'zingizning server nomiga o'zgartiring
```

### 3. Create database
```sql
-- SSMS da sql/schema.sql faylini oching va Execute bosing
```

### 4. Run ETL
```bash
# Test mode (3 sahifa, ~30 vakansiya):
# .env da TEST_MODE=true qiling

# To'liq yig'ish:
python src/main.py
```

### 5. Open Dashboard
```
Power BI Desktop → Open → hh_dashboard.pbix
```

---

## Power BI Views

After running the pipeline, these views are auto-created in SQL Server:

| View | Description |
|------|-------------|
| `vw_vacancies_full` | Main fact table with USD-normalized salaries |
| `vw_skill_demand` | Skill frequency → bar/treemap charts |
| `vw_salary_by_category` | Salary range by job category |
| `vw_daily_posting_trend` | Trend line + cumulative total |
| `vw_top_hiring_companies` | Company leaderboard |
| `vw_location_heatmap` | Geographic distribution |

**Connect Power BI:**
```
Get Data → SQL Server → 
Server: localhost\SQLEXPRESS
Database: headhunter
```

---

## Data Collected

| Column | Description |
|--------|-------------|
| `h_id` | Unique HeadHunter vacancy ID (dedup key) |
| `title` | Full vacancy title as posted |
| `position` | Inferred role (cleaned from title) |
| `category_en` | Professional area in English |
| `publish_date` | First posting date (YYYY-MM-DD) |
| `company` | Employer name |
| `skills` | Semicolon-separated required skills |
| `skill_type` | Technical / Soft / Language |
| `country` | Country of the role |
| `location` | City / region |
| `min_salary_usd` | Minimum salary normalized to USD |
| `max_salary_usd` | Maximum salary normalized to USD |

---

## Key Design Decisions

- **Modular** — collector / cleaner / loader are fully independent
- **Idempotent** — re-running never duplicates data (upsert by `h_id`)
- **Rate-safe** — exponential backoff on 429, configurable delay
- **Power BI-ready** — salary normalized to USD, skill types classified
- **Clean data** — Cyrillic company names and skills translated to English

---

## Tech Stack

| Layer | Tool |
|-------|------|
| Language | Python 3.11+ |
| HTTP | `requests` with retry |
| Transform | `pandas`, `numpy` |
| Database | SQL Server 2025 Express + `pyodbc` / `SQLAlchemy` |
| Config | `python-dotenv` |
| Analysis | Jupyter Notebook + `matplotlib` |
| Visualization | Power BI Desktop |

---

## .env Example

```env
SEARCH_TEXT=data analyst
AREA_ID=97
PER_PAGE=100
TEST_MODE=false
REQUEST_DELAY=0.2
DB_SERVER=localhost\SQLEXPRESS
DB_NAME=headhunter
DB_DRIVER=ODBC Driver 18 for SQL Server
DB_TRUSTED=yes
DB_CERT=yes
OUTPUT_DIR=output
LOG_LEVEL=INFO
```

---

## License

MIT — free to use, adapt, and extend.
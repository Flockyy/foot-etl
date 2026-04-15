# foot-etl

End-to-end World Cup data pipeline -- from raw CSV/JSON sources to a normalised star-schema database and KPI visualisations.

## Project structure

```
foot-etl/
├── data/                          # Raw data (gitignored)
│   ├── matches_19302010.csv       # World Cup matches 1930-2010 (incl. qualifiers)
│   ├── WorldCupMatches2014.csv    # World Cup 2014
│   ├── data_2018.json             # World Cup 2018 (stadiums, teams, TV)
│   └── worldcup.json              # World Cup 2022
├── models/                        # dbt SQL models (star schema)
│   ├── dim_city.sql
│   ├── dim_stadium.sql
│   ├── dim_team.sql
│   ├── dim_edition.sql
│   ├── fct_match.sql
│   └── schema.yml
├── seeds/                         # dbt seed CSVs
│   ├── matches_19302010.csv
│   ├── world_cup_matches_2014.csv
│   ├── worldcup_2022.csv
│   └── schema.yml
├── pipeline.py                    # Pandas ETL pipeline -> SQLite
├── main.py                        # dbt pipeline runner (seed -> run -> test)
├── data_exploration.ipynb         # Data loading & cleaning notebook
├── kpi.ipynb                      # KPI dashboard (reads from SQLite)
├── dbt_project.yml
├── profiles.yml
└── pyproject.toml
```

## Star schema

Both pipelines produce the same logical model -- 5 tables, 900 matches (WC finals only, no qualifiers, 1930-2022):

| Table | Rows | Description |
|-------|------|-------------|
| `city` / `dim_city` | 157 | Distinct host cities |
| `stadium` / `dim_stadium` | 163 | Stadiums with city FK |
| `team` / `dim_team` | 87 | All teams across all editions |
| `edition` / `dim_edition` | 21 | One row per tournament (year + host country) |
| `match` / `fct_match` | 900 | Fact table with home/away team, score, result, stadium and edition FKs |

## Pipelines

### 1. Pandas pipeline -> SQLite

```bash
python pipeline.py
```

Produces `foot_etl.sqlite` with tables: `city`, `stadium`, `team`, `edition`, `match`.

### 2. dbt pipeline -> DuckDB

```bash
python main.py
# or directly:
dbt seed && dbt run && dbt test
```

Produces `foot_etl.duckdb` with the same schema as above (prefixed `dim_` / `fct_`).
dbt test results: **PASS=21 WARN=0 ERROR=0**

## Setup

```bash
uv venv
uv sync
source .venv/bin/activate
```

Requires **Python 3.12** and [uv](https://github.com/astral-sh/uv).

## KPIs

Open `kpi.ipynb` after running `pipeline.py`. All KPIs query the unified `match` table via JOIN.

| # | KPI |
|---|-----|
| 1 | Average goals per match by edition (1930-2022) |
| 2 | Top 15 teams by total goals scored (all editions) |
| 3 | Win / draw / loss record + win rate -- top 10 teams |
| 4 | Most prolific editions (total goals & goals per match) |
| 5 | 2022 group stage: goals by group & goal difference per team |

## Data sources

| File | Content | Format |
|------|---------|--------|
| `matches_19302010.csv` | All WC matches 1930-2010 (finals + qualifiers) | CSV |
| `WorldCupMatches2014.csv` | WC 2014 with attendance & referee data | CSV (`;`) |
| `data_2018.json` | Stadiums, teams, TV channels 2018 | JSON |
| `worldcup.json` | Full match results 2022 | JSON |

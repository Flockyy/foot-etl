# foot-etl ⚽

End-to-end football data pipeline — from raw CSV/JSON sources to a clean SQLite database and KPI visualisations.

## Project structure

```
foot-etl/
├── data/                        # Raw data (gitignored)
│   ├── matches_19302010.csv     # World Cup matches 1930–2010
│   ├── WorldCupMatches2014.csv  # World Cup 2014
│   ├── data_2018.json           # World Cup 2018 (stadiums, teams, TV)
│   └── worldcup.json            # World Cup 2022
├── models/staging/              # dbt SQL + Python models
├── seeds/                       # dbt seed CSVs
├── pipeline.py                  # Pandas ETL pipeline → SQLite
├── main.py                      # dbt pipeline (seed → run → test)
├── data_exploration.ipynb       # Data loading & cleaning notebook
├── kpi.ipynb                    # KPI dashboard (reads from SQLite)
├── dbt_project.yml
├── profiles.yml
└── pyproject.toml
```

## Pipelines

### 1. Pandas pipeline → SQLite

Cleans and loads all sources into `foot_etl.sqlite`.

```bash
python pipeline.py
```

Produces tables: `stadiums`, `teams`, `tv_channels`, `matches`, `wc2014`, `wc2022`.

### 2. dbt pipeline → DuckDB

Runs dbt seeds + staging models into `foot_etl.duckdb`.

```bash
python main.py
```

Produces tables: `stg_matches`, `stg_world_cup_matches`, `stg_stadiums`, `stg_teams`, `stg_tv_channels`.

## Setup

```bash
# Create virtualenv and install dependencies
uv venv
uv sync

# Activate
source .venv/bin/activate
```

Requires **Python 3.12+** and [uv](https://github.com/astral-sh/uv).

## KPIs

Open `kpi.ipynb` after running `pipeline.py`. Includes:

- Goals per match over all World Cup editions (1930–2022)
- Top 15 teams by total goals scored
- Win / draw / loss record + win rate per team
- Attendance by city and stage (2014)
- Group stage goals and goal difference (2022)

## Data sources

| File | Content | Format |
|------|---------|--------|
| `matches_19302010.csv` | All WC matches 1930–2010 | CSV |
| `WorldCupMatches2014.csv` | WC 2014 with attendance | CSV (`;`) |
| `data_2018.json` | Stadiums, teams, TV channels 2018 | JSON |
| `worldcup.json` | Full match results 2022 | JSON |

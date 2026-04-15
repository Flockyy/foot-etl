"""
foot-etl · pandas pipeline → SQLite
Reuses the same cleaning logic as data_exploration.ipynb.
"""

import json
import re
import sqlite3
from pathlib import Path

import pandas as pd

DB_PATH = Path("foot_etl.sqlite")
DATA_DIR = Path("data")


# ─────────────────────────────────────────────────────────────
# Extract
# ─────────────────────────────────────────────────────────────

def extract() -> dict[str, pd.DataFrame]:
    # JSON (2018 World Cup)
    with open(DATA_DIR / "data_2018.json") as f:
        raw = json.load(f)

    stadiums  = pd.DataFrame(raw["stadiums"])
    teams     = pd.DataFrame(raw["teams"])
    tv        = pd.DataFrame(raw["tvchannels"])

    # CSV
    matches = pd.read_csv(DATA_DIR / "matches_19302010.csv")
    wc2014  = pd.read_csv(DATA_DIR / "WorldCupMatches2014.csv", delimiter=";")

    # JSON (2022 World Cup)
    with open(DATA_DIR / "worldcup.json") as f:
        raw2022 = json.load(f)
    wc2022 = pd.DataFrame(raw2022["matches"])

    return {
        "stadiums": stadiums,
        "teams": teams,
        "tv_channels": tv,
        "matches": matches,
        "wc2014": wc2014,
        "wc2022": wc2022,
    }


# ─────────────────────────────────────────────────────────────
# Transform 
# ─────────────────────────────────────────────────────────────

def _clean_team_name(name: str) -> str:
    return re.sub(r"\s*\(.*?\)", "", str(name)).strip()


def transform(dfs: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:

    # ── tv_channels ──────────────────────────────────────────
    tv = dfs["tv_channels"].copy()
    tv["lang"] = tv["lang"].apply(
        lambda v: ", ".join(v) if isinstance(v, list) else str(v)
    )

    # ── matches_19302010 ─────────────────────────────────────
    matches = dfs["matches"].copy()

    matches["team1"] = matches["team1"].apply(_clean_team_name)
    matches["team2"] = matches["team2"].apply(_clean_team_name)
    matches["venue"] = matches["venue"].str.rstrip(".").str.strip()

    score_split = matches["score"].str.extract(
        r"(?P<full_time>\d+-\d+)\s*\((?P<half_time>\d+-\d+)\)"
    )
    matches[["full_time_score", "half_time_score"]] = score_split

    before = len(matches)
    matches.drop_duplicates(inplace=True)
    print(f"matches — duplicates removed: {before - len(matches)}")

    matches.fillna(matches.select_dtypes(include="number").mean(), inplace=True)

    # ── WorldCupMatches2014 ──────────────────────────────────
    wc = dfs["wc2014"].copy()

    str_cols = wc.select_dtypes(include="str").columns
    wc[str_cols] = wc[str_cols].apply(lambda col: col.str.strip())

    wc["Home Team Name"] = wc["Home Team Name"].str.title()
    wc["Away Team Name"] = wc["Away Team Name"].str.title()
    wc["City"]           = wc["City"].str.title()

    wc["Datetime"] = pd.to_datetime(
        wc["Datetime"].str.strip(), format="%d %b %Y - %H:%M"
    )
    # SQLite doesn't store tz-aware datetimes — keep as naive string
    wc["Datetime"] = wc["Datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")

    wc["Attendance"] = wc["Attendance"].fillna(wc["Attendance"].median())

    before = len(wc)
    wc.drop_duplicates(inplace=True)
    print(f"wc2014 — duplicates removed: {before - len(wc)}")

    # ── worldcup.json (2022) ─────────────────────────────────
    wc22 = dfs["wc2022"].copy()

    # Flatten nested score dict
    wc22["score_ft_home"] = wc22["score"].apply(lambda s: s["ft"][0])
    wc22["score_ft_away"] = wc22["score"].apply(lambda s: s["ft"][1])
    wc22["score_ht_home"] = wc22["score"].apply(lambda s: s["ht"][0] if "ht" in s else None)
    wc22["score_ht_away"] = wc22["score"].apply(lambda s: s["ht"][1] if "ht" in s else None)
    wc22.drop(columns=["score", "goals1", "goals2"], inplace=True)

    wc22["date"] = pd.to_datetime(wc22["date"]).dt.strftime("%Y-%m-%d")

    before = len(wc22)
    wc22.drop_duplicates(inplace=True)
    print(f"wc2022 — duplicates removed: {before - len(wc22)}")

    return {
        "stadiums":    dfs["stadiums"],
        "teams":       dfs["teams"],
        "tv_channels": tv,
        "matches":     matches,
        "wc2014":      wc,
        "wc2022":      wc22,
    }


# ─────────────────────────────────────────────────────────────
# Load
# ─────────────────────────────────────────────────────────────

def load(dfs: dict[str, pd.DataFrame], db_path: Path = DB_PATH) -> None:
    with sqlite3.connect(db_path) as con:
        for table_name, df in dfs.items():
            df.to_sql(table_name, con, if_exists="replace", index=False)
            print(f"  ✓ {table_name:20s} — {len(df):>5} rows → {db_path.name}")


# ─────────────────────────────────────────────────────────────
# Pipeline
# ─────────────────────────────────────────────────────────────

def main() -> None:
    print("── foot-etl pandas pipeline ────────────────────────────────")

    print("\n[1/3] Extracting…")
    raw = extract()

    print("\n[2/3] Transforming…")
    clean = transform(raw)

    print("\n[3/3] Loading into SQLite…")
    load(clean)

    print(f"\n✓ Done. Database: {DB_PATH.resolve()}")


if __name__ == "__main__":
    main()

import subprocess
import sys
from pathlib import Path

PROJECT_DIR = Path(__file__).parent
PROFILES_DIR = PROJECT_DIR  # profiles.yml lives next to dbt_project.yml


def run_dbt(*args: str) -> None:
    """Run a dbt command inside the project directory."""
    dbt_bin = PROJECT_DIR / ".venv" / "bin" / "dbt"
    cmd = [str(dbt_bin), *args, "--profiles-dir", str(PROFILES_DIR)]
    result = subprocess.run(cmd, cwd=PROJECT_DIR)
    if result.returncode != 0:
        sys.exit(result.returncode)


def main() -> None:
    print("── foot-etl pipeline ──────────────────────────────────────────")

    # 1. Load CSV seeds into DuckDB
    print("\n[1/3] Loading seeds (CSV → DuckDB)...")
    run_dbt("seed")

    # 2. Run staging models (SQL + Python)
    #    stg_matches, stg_world_cup_matches, stg_stadiums, stg_teams, stg_tv_channels
    print("\n[2/3] Running staging models (clean & transform)...")
    run_dbt("run")

    # 3. Run tests
    print("\n[3/3] Running tests...")
    run_dbt("test")

    print("\n✓ Pipeline complete. DuckDB file: foot_etl.duckdb")


if __name__ == "__main__":
    main()


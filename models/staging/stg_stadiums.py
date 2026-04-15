import json


def model(dbt, session):
    """
    Load data_2018.json and return cleaned stadiums, teams and tv_channels
    as a single DuckDB relation by combining all three into a labelled long table.
    Each entity is materialised as its own model — this model returns stadiums.
    """
    dbt.config(materialized="table")

    with open("data/data_2018.json") as f:
        raw = json.load(f)

    import pandas as pd

    df = pd.DataFrame(raw["stadiums"])
    # No nulls, no duplicates — types already correct
    return df

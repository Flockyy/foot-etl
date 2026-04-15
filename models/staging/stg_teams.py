import json


def model(dbt, session):
    dbt.config(materialized="table")

    with open("data/data_2018.json") as f:
        raw = json.load(f)

    import pandas as pd

    df = pd.DataFrame(raw["teams"])
    # No nulls, no duplicates — types already correct
    return df

import json


def model(dbt, session):
    dbt.config(materialized="table")

    with open("data/data_2018.json") as f:
        raw = json.load(f)

    import pandas as pd

    df = pd.DataFrame(raw["tvchannels"])

    # 'lang' is stored as a Python list (e.g. ['da']) — flatten to comma-separated string
    df["lang"] = df["lang"].apply(
        lambda v: ", ".join(v) if isinstance(v, list) else str(v)
    )

    return df

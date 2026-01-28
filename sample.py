import pandas as pd

df = pd.read_csv("./all_asctb_ids_and_types.csv", dtype=str)

counts = (
    df.dropna(subset=["id"])
      .assign(id=df["id"].str.strip())
      .groupby(df["cf_asctb_type"].str.strip())
      ["id"].nunique()
)

print(counts)

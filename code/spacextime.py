import pandas as pd
import re

main = pd.read_csv("data/Urinary_system.csv", header=12, encoding="utf-8-sig")
main.columns = main.columns.str.strip()

# normalize
def normalize(val):
    if pd.isna(val):
        return "nan"
    val = str(val).lower().replace("–", "").replace("-", "").replace(" ", "")
    return val.strip()

main["TimeScale_norm"] = main["TimeScale"].apply(normalize)
main.columns = main.columns.str.strip()


# mapping
mapping = {
    "milliseconds": ["<1 second"],
    "seconds": ["1s - 1min"],
    "secondsminutes": ["1s - 1min", "1min - 1hr"],
    "minuteshours": ["1min - 1hr", "1hr - 1day"],
    "hoursdays": ["1hr - 1day", "1day - 1week"],
    "daysweeks": ["1day - 1week", "1 week - 1 year"],
    "hours": ["1hr - 1day"],
    "minutes": ["1min - 1hr"],
    "days": ["1day - 1week"],
    "nan": ["nan"],
}

structure_label_cols = [c for c in main.columns if re.search(r"Structure/\d+/LABEL", c)]
cell_label_cols = [c for c in main.columns if re.search(r"Cell/\d+/LABEL", c)]

cols = (
    ["Human Organ System", "Major Organs"]
    + structure_label_cols
    + cell_label_cols
    + [
        "<1 second",
        "1s - 1min",
        "1min - 1hr",
        "1hr - 1day",
        "1day - 1week",
        "1 week - 1 year",
        "1 year or longer",
        "nan",
    ]
)

output = pd.DataFrame(columns=cols)

for _, row in main.iterrows():
    system = row.get("Function/1", "")
    organ = row.get("Structure/1", "")
    process = row.get("Process triple", "")
    scale = row["TimeScale_norm"]

    new_row = {col: "" for col in cols}
    new_row["Human Organ System"] = system
    new_row["Major Organs"] = organ

    for c in structure_label_cols + cell_label_cols:
        new_row[c] = row.get(c, "")

    for t in mapping.get(scale, []):
        new_row[t] = process

    output.loc[len(output)] = new_row

output.to_csv("data/spacextime_table.csv", index=False, encoding="utf-8-sig")

print("Created successfully!")
import pandas as pd

# --- LOAD FINAL CSV ---
df = pd.read_csv("data/temporal_spatial_data/male_reproductive_lowest_type_from_asctb_matched_v3.csv", encoding="utf-8-sig")

# --- DEFINE TIME COLUMNS ---
time_cols = [
    "<1 second",
    "1s - < 1min",
    "1min - < 1hr",
    "1hr - < 1day",
    "1day - < 1week",
    "1 week - < 1 year",
    "1 year or longer",
]

# --- PREPARE MELTED DATA ---
melted = df.melt(
    id_vars=["Major Organs", "Lowest Type ASCTB Matched"],
    value_vars=time_cols,
    var_name="Time Range",
    value_name="Function@Process"
)

# Drop empty / NaN function@process
melted = melted.dropna(subset=["Function@Process"])
melted = melted[melted["Function@Process"].astype(str).str.strip() != ""]

# --- GROUP & COMBINE ---
# Aggregate by Organ + Time Range + Type
grouped = (
    melted.groupby(["Time Range", "Major Organs", "Lowest Type ASCTB Matched"])["Function@Process"]
    .apply(lambda x: "; ".join(sorted(x.unique())))
    .reset_index()
)

# --- PIVOT TO WIDE TABLE ---
pivot = grouped.pivot_table(
    index=["Time Range", "Major Organs"],
    columns="Lowest Type ASCTB Matched",
    values="Function@Process",
    aggfunc=lambda x: "; ".join(sorted(set(x))),
    fill_value=""
).reset_index()

# --- ENSURE COLUMN ORDER ---
desired_types = ["AS", "FTU", "CT", "B", "Unknown"]
for t in desired_types:
    if t not in pivot.columns:
        pivot[t] = ""

pivot = pivot[["Time Range", "Major Organs"] + desired_types]

# --- SAVE OUTPUT ---
pivot.to_csv("output/temporal_spatial_output/male_reproductive_spatial_temporal_v2.csv", index=False, encoding="utf-8-sig")

print("Created summary table by Time Range × Organ × Type!")
print("Total rows:", len(pivot))
print(pivot.head(10))

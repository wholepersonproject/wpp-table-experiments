#!/usr/bin/env python3
import pandas as pd
import glob
import os
import re

# --------------------
# CONFIG - change only these
# --------------------
INPUT_FOLDER = "./data/WPP Input Tables/"   # root folder containing CSV files (will search recursively)
OUTPUT_FOLDER = "./output/temporal_spatial_output/v6/"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# --------------------
# Your original mappings / sets (kept unchanged)
# --------------------
TIME_COLUMNS = [
    "<1 second", "1s - < 1min", "1min - < 1hr", "1hr - < 1day", "1day - < 1week",
    "1 week - < 1 year", "1 year or longer",
]

TIME_MAPPING = {
    "milliseconds": ["<1 second"], "seconds": ["1s - < 1min"], "secondsminutes": ["1s - < 1min", "1min - < 1hr"],
    "minuteshours": ["1min - < 1hr", "1hr - < 1day"], "hoursdays": ["1hr - < 1day", "1day - < 1week"],
    "daysweeks": ["1day - < 1week", "1 week - < 1 year"], "hours": ["1hr - < 1day"], "minutes": ["1min - < 1hr"],
    "days": ["1day - < 1week"], "nan": ["Unknown"],
    "weeks": ["1 week - < 1 year"], "months": ["1 week - < 1 year"], "years": ["1 year or longer"],
    "weeksmonths": ["1 week - < 1 year"], "minuteshoursdays": ["1min - < 1hr", "1hr - < 1day", "1day - < 1week"],
    "hoursdaysweeksmonths": ["1hr - < 1day", "1day - < 1week", "1 week - < 1 year"],
    "secondsminuteshours": ["1s - < 1min", "1min - < 1hr", "1hr - < 1day"],
    "milisecondsseconds": ["<1 second", "1s - < 1min"], "secondshours": ["1s - < 1min", "1min - < 1hr", "1hr - < 1day"],
    "continuous": ["continuous"], "variable": ["variable"],
}

SPATIAL_MAPPING = {
    "tissue": "AS",
    "tissueftu": "FTU",
    "cell": "CT",
    "organ": "Organ",
    "organsystem": "Organ",
    "biomolecule": "B",
    "molecule": "B",
    "subcellular": "Unknown",
    "organism": "Unknown",
    "nan": "Unknown",
    "": "Unknown"
}

time_category_order = [
    "<1 second", "1s - < 1min", "1min - < 1hr", "1hr - < 1day",
    "1day - < 1week", "1 week - < 1 year", "1 year or longer",
    "continuous", "variable"
]

ftu_ids = {
    "UBERON:0004203",
    "UBERON:0001289",
    "UBERON:0004205",
    "UBERON:0004193",
    "UBERON:0001285",
    "UBERON:0004204",
    "UBERON:0001229",
    "UBERON:0001291",
    "UBERON:0004647",
    "UBERON:0002299",
    "UBERON:8410043",
    "UBERON:0000006",
    "UBERON:0001263",
    "UBERON:0014725",
    "UBERON:0004179",
    "UBERON:0001983",
    "UBERON:0000412",
    "UBERON:0002073",
    "UBERON:0013487",
    "UBERON:0001213",
    "UBERON:0001250",
    "UBERON:0001959",
    "UBERON:0002125",
    "UBERON:0001831",
    "UBERON:0001832",
    "UBERON:0001736",
}

# --------------------
# Helper functions (robustified)
# --------------------
def find_col_case_insensitive(columns, candidates):
    """
    Return first matching column name from 'columns' for any candidate (case-insensitive), or None.
    """
    lowered = {c.lower(): c for c in columns}
    for cand in candidates:
        if cand in columns:
            return cand
        if cand.lower() in lowered:
            return lowered[cand.lower()]
    return None

def clean_effector_id(eff_id):
    """
    Normalize an Effector/ID value so it can be matched to ftu_ids.
    - extracts a 'UBERON:NNNN' token if present (case-insensitive),
    - strips common wrappers, removes URLs, then uppercases fallback string.
    - returns None if nothing meaningful.
    """
    if pd.isna(eff_id):
        return None
    s = str(eff_id).strip()
    if not s:
        return None
    # extract UBERON token if present
    m = re.search(r"(UBERON:\d+)", s, flags=re.IGNORECASE)
    if m:
        return m.group(1).upper()
    # remove urls and surrounding punctuation then uppercase
    s2 = re.sub(r"https?://\S+", "", s)
    s2 = re.sub(r"[<>()\[\]{}\"']", "", s2)
    s2 = s2.strip()
    return s2.upper() if s2 else None

def normalize_time(val):
    if pd.isna(val):
        return "nan"
    val = str(val).lower()
    val = re.sub(r"[–,\-\s]", "", val)
    return val.strip()

# def normalize_spatial(val, row, effector_id_col=None, ftu_ids_up=None):
#     """
#     Robust spatial normalization:
#       - uses SPATIAL_MAPPING for direct keys,
#       - if key starts with 'tissue' will check effector id column (case insensitive) for FTU membership,
#       - accepts effector_id_col name (string) and ftu_ids_up (upper-cased set) to compare.
#     """
#     if pd.isna(val) or str(val).strip() == "":
#         return SPATIAL_MAPPING.get("nan", "Unknown")
#     val_str = str(val).strip().lower()
#     normalized_key = re.sub(r"[^a-z0-9]", "", val_str)
#     if normalized_key in SPATIAL_MAPPING:
#         if normalized_key == "tissueftu":
#             return "FTU"
#         return SPATIAL_MAPPING[normalized_key]
#     if normalized_key.startswith("tissue"):
#         # consult effector id if provided
#         if effector_id_col is not None and effector_id_col in row.index:
#             raw_eff = row.get(effector_id_col)
#             cleaned = clean_effector_id(raw_eff)
#             if cleaned and ftu_ids_up and cleaned in ftu_ids_up:
#                 return "FTU"
#         return "AS"
#     return SPATIAL_MAPPING.get(normalized_key, "Unknown")

def normalize_spatial(val, effector_id=None):
    if pd.isna(val) or str(val).strip() == "":
        return SPATIAL_MAPPING.get("nan", "Unknown")
    v = re.sub(r"[^a-z0-9]", "", str(val).strip().lower())
    if v == "tissueftu":
        return "FTU"
    if v.startswith("tissue"):
        if effector_id is not None and pd.notna(effector_id):
            eff_id_str = str(effector_id).strip()
            if eff_id_str in ftu_ids:
                return "FTU"
        return "AS"
    return SPATIAL_MAPPING.get(v, "Unknown")

def get_lowest_function(row):
    lowest_func = ""
    function_cols = [col for col in row.index if re.match(r"Function/\d+$", col.strip())]
    function_cols.sort(key=lambda c: int(re.search(r"\d+", c).group()))
    for col in function_cols:
        val = str(row.get(col, "")).strip()
        if pd.notna(val) and val.lower() != "nan" and val != "":
            lowest_func = val
    return lowest_func if lowest_func else "Unknown"

def build_combined_process(row):
    proc = row.get("Process", "")
    if pd.isna(proc) or str(proc).strip().lower() in ["", "nan", "none", "null"]:
        return None   # ← SKIP MISSING ENTIRELY

    proc = str(proc).strip()

    lf = row.get("Lowest_Function", "")
    if lf and lf != "Unknown":
        return f"{lf}@{proc}"
    else:
        return proc

def process_and_save_single(MAIN_CSV_PATH, OUTPUT_PATH):
    main = pd.read_csv(MAIN_CSV_PATH, header=header_row, encoding="utf-8-sig")
    main.columns = main.columns.str.strip()

    # normalize TimeScale and compute Lowest_Function & Combined_Process
    main["TimeScale_norm"] = main["TimeScale"].apply(normalize_time)
    main["Lowest_Function"] = main.apply(get_lowest_function, axis=1)
    main["Combined_Process"] = main.apply(build_combined_process, axis=1)

    # find effector id column case-insensitively once per file
    effector_id_col = find_col_case_insensitive(main.columns, ["Effector/ID","Effector ID","Effector_ID","Effector/Id","Effector/identifier","EffectorID"])
    # prepare uppercase FTU id set for robust comparison
    ftu_ids_up = {x.upper() for x in ftu_ids} if ftu_ids else set()

    # compute Spatial_Type using robust normalizer
    # main["Spatial_Type"] = main.apply(
    #     lambda row: normalize_spatial(row.get("EffectorScale", ""), row, effector_id_col, ftu_ids_up),
    #     axis=1
    # )
    main["Spatial_Type"] = main.apply(lambda r: normalize_spatial(r.get("EffectorScale", ""), r.get("Effector/ID", "")), axis=1)

    # expand time ranges (some entries map to multiple TIME_MAPPING entries)
    melted_df = main.copy()
    melted_df["Time Range"] = melted_df["TimeScale_norm"].apply(lambda x: TIME_MAPPING.get(x, ["Unknown"]))
    melted_df = melted_df.explode("Time Range")

    # dedupe and build Function@Process strings per (Time Range, Spatial_Type)
    grouped = (
        melted_df.groupby(["Time Range", "Spatial_Type"])["Combined_Process"]
        .apply(
            lambda x: "? ".join(
                sorted(
                    set(
                        p.strip() for process_str in x.dropna()
                        for p in process_str.split('?') if p.strip()
                    )
                )
            )
        )
        .reset_index(name="Function@Process")
    )

    # drop empty/Unknown groups
    grouped = grouped[grouped["Function@Process"] != ""]
    grouped = grouped[grouped["Function@Process"] != "Unknown"]

    # pivot into spatial x temporal table
    pivot = grouped.pivot(
        index="Time Range",
        columns="Spatial_Type",
        values="Function@Process"
    ).fillna("").reset_index()

    # remove Unknown column if present
    if "Unknown" in pivot.columns:
        pivot = pivot.drop(columns=["Unknown"])

    desired_spatial_types = ["Organ", "AS", "FTU", "CT", "B"]
    for t in desired_spatial_types:
        if t not in pivot.columns:
            pivot[t] = ""

    pivot = pivot.set_index("Time Range").reindex(time_category_order).fillna("").reset_index()
    pivot["Time Range"] = pd.Categorical(pivot["Time Range"], categories=time_category_order, ordered=True)
    pivot = pivot.sort_values("Time Range").reset_index(drop=True)

    final_pivot = pivot[["Time Range"] + desired_spatial_types]

    final_pivot.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")

# --------------------
# Walk all folders under INPUT_FOLDER and process every CSV found (logic unchanged)
# --------------------
csv_files = sorted(glob.glob(os.path.join(INPUT_FOLDER, "**", "*.csv"), recursive=True))
if not csv_files:
    print("No CSV files found in", INPUT_FOLDER)
    raise SystemExit(1)

for file_path in csv_files:
    file_name = os.path.basename(file_path)
    if "endocrine" in file_name.lower():
        header_row = 12
    else:
        header_row = 11
    base_noext = os.path.splitext(file_name)[0]
    words = re.findall(r"\w+", base_noext)
    if len(words) >= 2:
        prefix = f"{words[0]}_{words[1]}"
    elif len(words) == 1:
        prefix = words[0]
    else:
        prefix = base_noext

    out_name = f"{prefix}_spatial_temporal_table.csv"
    out_path = os.path.join(OUTPUT_FOLDER, out_name)

    try:
        process_and_save_single(file_path, out_path)
        print(f"Saved: {out_path}")
    except Exception as e:
        # preserve original logic; avoid stopping on one bad file
        print(f"Failed processing {file_name}: {e}")
        continue

print("Done processing all folders.")

#!/usr/bin/env python3
"""
Compare WPP Uberon IDs (AS_ID) to ASTCB/HRA master and output two CSVs:
 - present (IDs found in ASTCB) and
 - missing (IDs not found in ASTCB).

Each output row includes:
 - AS_ID (canonical UBERON:0000000 form)
 - WPP_LABELS (all labels seen in the tissue input file for that ID)
 - WPP_SOURCES (all detected source/table/file column values from the tissue input file)
 - WPP_RAW (raw representations seen in tissue file)

Notes:
 - Labels and sources are collected only from the tissue input file.
 - ASTCB master is used only to determine presence/absence (no ASTCB labels are emitted).
"""

import os
import re
import pandas as pd

# ---------- CONFIG ----------
tissue_input_file = "./output/analysis/all_Uberon_statistics/AS_UBERON_in_WPP.csv"
astcb_master_file  = "./data/all_asctb_ids_and_types.csv"
output_present_file = "./output/analysis/all_Uberon_statistics/uberon_ids_present_in_astcb.csv"
output_missing_file = "./output/analysis/all_Uberon_statistics/uberon_ids_missing_in_asctb.csv"

ID_SEPARATOR = ";"

# Candidate label column names (search tissue file)
LABEL_COL_CANDIDATES = [
    "label", "name", "as_label", "AS", "AS_LABEL", "pref_label", "preferred_label"
]

# Candidate ASTCB ID header names (used to detect which column contains IDs)
ASTCB_ID_COL_CANDIDATES = ["id", "ID", "uberon_id", "Uberon", "Uberon ID"]

# helpers
_uberon_digits_re = re.compile(r"(\d+)")

def clean_text(val):
    if pd.isna(val):
        return None
    s = str(val).strip()
    if s == "":
        return None
    return " ".join(s.split())

def split_ids_field(id_field, sep=ID_SEPARATOR):
    if pd.isna(id_field) or id_field is None or str(id_field).strip() == "":
        return []
    parts = [p.strip() for p in str(id_field).split(sep)]
    return [p for p in parts if p]

def is_cl_id(idstr):
    return idstr and str(idstr).strip().upper().startswith("CL")

def normalize_to_uberon(idstr):
    if idstr is None:
        return None
    s = str(idstr).strip()
    if s == "":
        return None
    s_upper = s.upper()
    m = re.match(r"^UBERON[:_]?0*([0-9]+)$", s_upper)
    if m:
        num = int(m.group(1))
        return f"UBERON:{num:07d}"
    # if prefix is not UBERON and has colon, treat as non-uberon
    if re.match(r"^[A-Z]+[:_].*$", s_upper):
        prefix = s_upper.split(":", 1)[0].split("_", 1)[0]
        if prefix != "UBERON":
            return None
    md = _uberon_digits_re.search(s)
    if not md or len(md.group(1)) < 4:
        return None
    num = int(md.group(1))
    return f"UBERON:{num:07d}"

def find_column(df, candidates):
    lowered = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand in df.columns:
            return cand
        lc = cand.lower()
        if lc in lowered:
            return lowered[lc]
    return None

def detect_source_columns(df):
    # heuristic: pick columns that contain 'source' or 'table' or 'file' (case-insensitive)
    return [c for c in df.columns if re.search(r"(source|table|file)", c, re.I) and c.lower() not in ("as","as_id","as id")]

def join_unique(iterable):
    # preserve insertion order of unique items, return joined string or empty string
    seen = []
    for x in iterable:
        if x is None:
            continue
        s = str(x).strip()
        if s and s not in seen:
            seen.append(s)
    return " | ".join(seen)

def main():
    # check inputs
    if not os.path.exists(tissue_input_file):
        print(f"[ERROR] tissue input file not found: {tissue_input_file}")
        return
    if not os.path.exists(astcb_master_file):
        print(f"[ERROR] astcb master file not found: {astcb_master_file}")
        return

    wpp_df = pd.read_csv(tissue_input_file, dtype=str)
    astcb_df = pd.read_csv(astcb_master_file, dtype=str)

    # detect ID column in tissue file (prefer AS_ID)
    if "AS_ID" in wpp_df.columns:
        wpp_id_col = "AS_ID"
    else:
        wpp_id_col = next((c for c in wpp_df.columns if "id" in c.lower()), None)

    if wpp_id_col is None:
        print("[ERROR] Could not find an ID-like column in tissue input file. Columns:", wpp_df.columns.tolist())
        return

    # detect tissue label column (from tissue file only)
    wpp_label_col = find_column(wpp_df, LABEL_COL_CANDIDATES)

    # detect tissue source columns (heuristic)
    wpp_source_cols = detect_source_columns(wpp_df)

    # build maps from canonical Uberon -> sets of labels/sources/raws seen in tissue file
    wpp_uberon_set = set()
    wpp_labels_map = {}
    wpp_sources_map = {}
    wpp_raw_map = {}
    wpp_cl_ids = set()
    wpp_non_uberon = set()

    for _, row in wpp_df.iterrows():
        raw_field = row.get(wpp_id_col)
        ids = split_ids_field(raw_field, sep=ID_SEPARATOR)
        row_label = clean_text(row.get(wpp_label_col)) if wpp_label_col else None
        # collect all source values for this row (concatenate columnname=value so it's clear)
        source_values = []
        for sc in wpp_source_cols:
            v = clean_text(row.get(sc))
            if v:
                source_values.append(v)

        for raw in ids:
            raw_clean = clean_text(raw)
            if not raw_clean:
                continue
            if is_cl_id(raw_clean):
                wpp_cl_ids.add(raw_clean)
                continue
            norm = normalize_to_uberon(raw_clean)
            if norm:
                wpp_uberon_set.add(norm)
                wpp_labels_map.setdefault(norm, set())
                if row_label:
                    wpp_labels_map[norm].add(row_label)
                wpp_sources_map.setdefault(norm, set())
                if source_values:
                    wpp_sources_map[norm].update(source_values)
                wpp_raw_map.setdefault(norm, set()).add(raw_clean)
            else:
                wpp_non_uberon.add(raw_clean)

    # detect ASTCB ID column
    astcb_id_col = find_column(astcb_df, ASTCB_ID_COL_CANDIDATES)
    if astcb_id_col is None:
        astcb_id_col = next((c for c in astcb_df.columns if "id" in c.lower()), None)
        if astcb_id_col is None:
            print("[ERROR] Could not detect ID column in ASTCB master. Columns:", astcb_df.columns.tolist())
            return
        print(f"[INFO] Using guessed ASTCB ID column: {astcb_id_col}")

    # build canonical set of Uberons from ASTCB (use all rows; you can filter by type externally if desired)
    astcb_uberon_set = set()
    for val in astcb_df[astcb_id_col].dropna().astype(str):
        raw = val.strip()
        if is_cl_id(raw):
            continue
        norm = normalize_to_uberon(raw)
        if norm:
            astcb_uberon_set.add(norm)

    # compare
    present_ids = sorted(wpp_uberon_set & astcb_uberon_set)
    missing_ids = sorted(wpp_uberon_set - astcb_uberon_set)

    # prepare rows: only using WPP labels/sources/raws
    present_rows = []
    for uid in present_ids:
        present_rows.append({
            "AS_ID": uid,
            "WPP_LABELS": join_unique(sorted(wpp_labels_map.get(uid, []))),
            "WPP_SOURCES": join_unique(sorted(wpp_sources_map.get(uid, []))),
            # "WPP_RAW": join_unique(sorted(wpp_raw_map.get(uid, [])))
        })

    missing_rows = []
    for uid in missing_ids:
        missing_rows.append({
            "AS_ID": uid,
            "WPP_LABELS": join_unique(sorted(wpp_labels_map.get(uid, []))),
            "WPP_SOURCES": join_unique(sorted(wpp_sources_map.get(uid, []))),
            # "WPP_RAW": join_unique(sorted(wpp_raw_map.get(uid, [])))
        })

    # write outputs
    os.makedirs(os.path.dirname(output_present_file) or ".", exist_ok=True)
    os.makedirs(os.path.dirname(output_missing_file) or ".", exist_ok=True)

    pd.DataFrame(present_rows).to_csv(output_present_file, index=False)
    pd.DataFrame(missing_rows).to_csv(output_missing_file, index=False)

    # final printed stats (exact format requested)
    total_uberon_in_wpp = len(wpp_uberon_set)
    wpp_intersection_hra = len(present_ids)
    only_in_wpp = len(missing_ids)
    total_in_hra = len(astcb_uberon_set)
    only_in_hra = max(0, total_in_hra - wpp_intersection_hra)

    print(f"- Total Uberon ids in WPP => {total_uberon_in_wpp}")
    print(f"- WPP intersection HRA => {wpp_intersection_hra}")
    print(f"- Only in WPP => {only_in_wpp}")
    print(f"- Total in HRA => {total_in_hra}")
    print(f"- Only in HRA => {only_in_hra}")

if __name__ == "__main__":
    main()

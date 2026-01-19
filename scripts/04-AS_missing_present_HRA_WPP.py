#!/usr/bin/env python3
"""
Compare precomputed WPP Uberon IDs (AS, AS_ID) to ASTCB/HRA master.

Expect input:
 - WPP file: ./output/analysis/all_Uberon_statistics/all_Uberon_ids_in_WPP2.csv
   with columns: AS, AS_ID   (AS_ID may be semicolon-separated IDs)

 - ASTCB master: ./data/all_asctb_ids_and_types.csv

Outputs:
 - ./output/analysis/all_Uberon_statistics/uberon_ids_present_in_astcb2.csv
 - ./output/analysis/all_Uberon_statistics/uberon_ids_missing_in_asctb2.csv

Final printed stats (exact format):
- Total Uberon ids in WPP => ...
- WPP intersection HRA => ...
- Only in WPP => ...
- Total in HRA => ...
- Only in HRA => ...
"""

import os
import re
import pandas as pd

# INPUT / OUTPUT paths (adjust if needed)
tissue_input_file = "./output/analysis/all_Uberon_statistics/AS_UBERON_in_WPP.csv"
astcb_master_file = "./data/all_asctb_ids_and_types.csv"
output_missing_file = "./output/analysis/all_Uberon_statistics/uberon_ids_missing_in_asctb.csv"
output_present_file = "./output/analysis/all_Uberon_statistics/uberon_ids_present_in_astcb.csv"

ID_SEPARATOR = ";"

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
    """
    Normalize a raw id string to canonical 'UBERON:0002040' (7-digit zero-padded),
    or return None if it should not be treated as an Uberon.
    """
    if idstr is None:
        return None
    s = str(idstr).strip()
    if s == "":
        return None
    s_upper = s.upper()

    # direct UBERON forms, allowing leading zeros or underscore
    m = re.match(r"^UBERON[:_]?0*([0-9]+)$", s_upper)
    if m:
        num = int(m.group(1))
        return f"UBERON:{num:07d}"

    # if it starts with a different ontology prefix (e.g., FMA:, CL:), reject
    if re.match(r"^[A-Z]+[:_].*$", s_upper):
        prefix = s_upper.split(":", 1)[0].split("_", 1)[0]
        if prefix != "UBERON":
            return None
        # otherwise fall through

    # fallback: find a digit sequence of reasonable length and treat as Uberon numeric id
    md = _uberon_digits_re.search(s)
    if not md:
        return None
    if len(md.group(1)) < 4:
        return None
    num = int(md.group(1))
    return f"UBERON:{num:07d}"

def find_id_column(df, candidates):
    lowered = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand in df.columns:
            return cand
        lc = cand.lower()
        if lc in lowered:
            return lowered[lc]
    return None

def main():
    # 1) Read WPP precomputed file (AS, AS_ID)
    if not os.path.exists(tissue_input_file):
        print(f"[ERROR] WPP tissue input file not found: {tissue_input_file}")
        return

    wpp_df = pd.read_csv(tissue_input_file, dtype=str)
    if "AS_ID" not in wpp_df.columns and not any("id" in c.lower() for c in wpp_df.columns):
        print("[ERROR] Input WPP file does not appear to contain an AS_ID column.")
        print("Columns found:", wpp_df.columns.tolist())
        return

    # detect AS_ID-like column if name differs
    wpp_id_col = "AS_ID" if "AS_ID" in wpp_df.columns else next((c for c in wpp_df.columns if "id" in c.lower()), None)

    # Build set of canonical Uberon IDs from WPP AS_ID column.
    wpp_uberon_set = set()
    wpp_non_uberon = set()
    wpp_cl_ids = set()
    # iterate rows and split AS_ID field (may contain multiple separated by ';')
    for _, row in wpp_df.iterrows():
        raw_field = row.get(wpp_id_col)
        ids = split_ids_field(raw_field, sep=ID_SEPARATOR)
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
            else:
                wpp_non_uberon.add(raw_clean)

    # 2) Load ASTCB master
    if not os.path.exists(astcb_master_file):
        print(f"[ERROR] ASTCB master file not found: {astcb_master_file}")
        return

    astcb_df = pd.read_csv(astcb_master_file, dtype=str)

    # detect cf_asctb_type column (case-insensitive)
    lowered_cols = {c.lower(): c for c in astcb_df.columns}
    cf_type_col = None
    if "cf_asctb_type" in astcb_df.columns:
        cf_type_col = "cf_asctb_type"
    elif "cf_asctb_type" in lowered_cols:
        cf_type_col = lowered_cols["cf_asctb_type"]

    # detect ASTCB ID column for unique-by-type counts
    astcb_id_col = find_id_column(astcb_df, ASTCB_ID_COL_CANDIDATES)
    if astcb_id_col is None:
        astcb_id_col = next((c for c in astcb_df.columns if "id" in c.lower()), None)
        if astcb_id_col is None:
            print("[ERROR] Could not detect ID column in ASTCB master. Columns found:", astcb_df.columns.tolist())
            return
        print(f"[INFO] Using guessed ASTCB ID column: {astcb_id_col}")

    # 3) Compute unique raw ID counts per cf_asctb_type (Option 1)
    if cf_type_col:
        astcb_df["_cf_asctb_type_norm"] = astcb_df[cf_type_col].astype(str).str.strip().str.upper().fillna("")
        type_counts_df = (
            astcb_df.groupby("_cf_asctb_type_norm")[astcb_id_col]
                   .nunique()
                   .reset_index(name="unique_id_count")
        )
        print("\nASTCB unique ID counts by cf_asctb_type (normalized):")
        print(type_counts_df.to_string(index=False))
        total_as_unique_raw = int(type_counts_df.loc[type_counts_df["_cf_asctb_type_norm"] == "AS", "unique_id_count"].values[0]) if "AS" in type_counts_df["_cf_asctb_type_norm"].values else 0
    else:
        print("\n[INFO] No 'cf_asctb_type' column found in ASTCB master; cannot produce unique-by-type counts.")
        total_as_unique_raw = 0

    # 4) Filter ASTCB to rows with type == 'AS' for canonical normalization & comparison
    if cf_type_col:
        astcb_filtered = astcb_df[astcb_df["_cf_asctb_type_norm"] == "AS"].copy()
        print(f"[INFO] Filtering ASTCB to rows where {cf_type_col} == 'AS' -> {len(astcb_filtered)} rows retained.")
    else:
        astcb_filtered = astcb_df
        print(f"[INFO] No {cf_type_col} column; using all ASTCB rows ({len(astcb_filtered)}) for comparison.")

    # 5) Extract IDs from filtered ASTCB rows and normalize to canonical Uberon
    astcb_raw_ids = [str(v).strip() for v in astcb_filtered[astcb_id_col].dropna().astype(str) if str(v).strip()]
    astcb_uberon_set = set()
    astcb_non_uberon = set()
    astcb_cl_ids = set()
    for r in astcb_raw_ids:
        if is_cl_id(r):
            astcb_cl_ids.add(r)
            continue
        norm = normalize_to_uberon(r)
        if norm:
            astcb_uberon_set.add(norm)
        else:
            astcb_non_uberon.add(r)

    # 6) Compare canonical sets
    present_ids = sorted(wpp_uberon_set & astcb_uberon_set)
    missing_ids = sorted(wpp_uberon_set - astcb_uberon_set)

    # 7) Save present and missing (canonical IDs)
    os.makedirs(os.path.dirname(output_present_file) or ".", exist_ok=True)
    os.makedirs(os.path.dirname(output_missing_file) or ".", exist_ok=True)

    pd.DataFrame({"Present_AS_ID": present_ids}).to_csv(output_present_file, index=False)
    pd.DataFrame({"Missing_AS_ID": missing_ids}).to_csv(output_missing_file, index=False)

    # 8) Print diagnostic summary and final requested counts
    print("\n=== Diagnostic ===")
    print(f"Unique canonical Uberon IDs in WPP (from AS_ID): {len(wpp_uberon_set)}")
    print(f"Filtered CL IDs found in WPP AS_IDs: {len(wpp_cl_ids)}")
    print(f"Non-Uberon strings seen in WPP AS_IDs (sample up to 10): {list(sorted(wpp_non_uberon))[:10]} (count={len(wpp_non_uberon)})")
    print(f"Unique canonical Uberon IDs in ASTCB (from AS rows after normalization): {len(astcb_uberon_set)}")
    print(f"Filtered CL IDs found in ASTCB AS rows: {len(astcb_cl_ids)}")
    print(f"Non-Uberon strings seen in ASTCB AS rows (sample up to 10): {list(sorted(astcb_non_uberon))[:10]} (count={len(astcb_non_uberon)})")

    # Final requested counts (Uberon-only comparisons + Option1 total)
    total_uberon_in_wpp = len(wpp_uberon_set)
    wpp_intersection_hra = len(wpp_uberon_set & astcb_uberon_set)
    only_in_wpp = len(wpp_uberon_set - astcb_uberon_set)
    total_in_hra_raw_unique = total_as_unique_raw    # Option1 unique raw IDs for cf_asctb_type == "AS"
    only_in_hra_canonical = total_in_hra_raw_unique - wpp_intersection_hra

    print("\n=== Final counts ===")
    print(f"- Total Uberon ids in WPP => {total_uberon_in_wpp}")
    print(f"- WPP intersection HRA => {wpp_intersection_hra}")
    print(f"- Only in WPP => {only_in_wpp}")
    print(f"- Total in HRA => {total_in_hra_raw_unique}")
    print(f"- Only in HRA => {only_in_hra_canonical}")

if __name__ == "__main__":
    main()

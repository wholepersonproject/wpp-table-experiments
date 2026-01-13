#!/usr/bin/env python3
"""
scan_ftu_ids.py

Scan input tables (CSV/TSV/Excel) for a set of FTU UBERON IDs appearing in
EffectorLocation/ID or Effector/ID columns. Capture labels and counts and
write a summary CSV.

Usage:
    python scan_ftu_ids.py

Edit INPUT_FOLDER and OUT_CSV below, or pass them as env/args if you prefer.
"""

import os
import re
import glob
import argparse
from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd

# ---------------------------
# CONFIG (edit as needed)
# ---------------------------
INPUT_FOLDER = "./data/WPP Input Tables"   # folder to search (recursive)
OUT_CSV = "./output/unique_ftus/ftu_id_matches_summary.csv"
RECURSIVE = True
# Candidate ID columns to look for
ID_COLUMN_CANDIDATES = ["EffectorLocation/ID", "Effector/ID"]
LABEL_COLUMN_CANDIDATES = ["EffectorLocation/LABEL", "Effector/LABEL"]
# separators used when a cell contains multiple IDs in one cell
ID_SEPARATORS_REGEX = r"[;|,]\s*"

# Example FTU IDs set - replace or load dynamically as you wish
FTU_IDS = {
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

# ---------------------------
# Helpers
# ---------------------------
def derive_table_name(filepath: str) -> str:
    """
    Derive a table name from the filename: first two 'words' (alphanumeric + underscores).
    """
    stem = Path(filepath).stem
    parts = re.split(r'[\W_]+', stem)
    parts = [p for p in parts if p]
    if not parts:
        return stem
    return " ".join(parts[:2])

def find_best_column(columns: List[str], candidates: List[str]) -> Optional[str]:
    """
    Return the first matching column name from `columns` for any candidate in `candidates`.
    Matching is:
      - exact case-sensitive match
      - case-insensitive match
      - column that endswith the candidate (case-insensitive)
      - column that contains the candidate (case-insensitive)
    """
    lowered = {c.lower(): c for c in columns}
    # exact / case-insensitive
    for cand in candidates:
        if cand in columns:
            return cand
        lc = cand.lower()
        if lc in lowered:
            return lowered[lc]
    # suffix / contains matches
    for cand in candidates:
        lc = cand.lower()
        for c in columns:
            cl = c.lower()
            if cl.endswith(lc) or lc in cl:
                return c
    return None

def split_ids_from_cell(cell_value) -> List[str]:
    """
    Given a cell value (possibly numeric or float), return a list of trimmed ID strings.
    """
    if pd.isna(cell_value):
        return []
    s = str(cell_value).strip()
    if not s:
        return []
    parts = re.split(ID_SEPARATORS_REGEX, s)
    return [p.strip() for p in parts if p.strip()]

# ---------------------------
# Core scanning function
# ---------------------------
def scan_files(input_folder: str, ftu_ids: set, out_csv: str, recursive: bool = True):
    patterns = ["**/*.csv", "**/*.tsv", "**/*.xlsx", "**/*.xls"] if recursive else ["*.csv","*.tsv","*.xlsx","*.xls"]
    base = Path(input_folder)
    if not base.exists():
        raise FileNotFoundError(f"Input folder not found: {input_folder}")

    found_files = []
    for pat in patterns:
        found_files.extend([str(p) for p in base.glob(pat)])
    found_files = sorted(set(found_files))

    if not found_files:
        print(f"No files found under {input_folder}. Patterns: {patterns}")
        return pd.DataFrame()  # empty

    records = []  # collected match records

    for fp in found_files:
        fp_path = Path(fp)
        ext = fp_path.suffix.lower()
        table_name = derive_table_name(fp)
        try:
            if ext in (".xls", ".xlsx"):
                # handle each sheet
                xls = pd.ExcelFile(fp)
                for sheet in xls.sheet_names:
                    try:
                        df = xls.parse(sheet, dtype=str)
                    except Exception as e:
                        # skip unreadable sheet
                        records.append({
                            "input_file": fp,
                            "sheet": sheet,
                            "table_name": table_name,
                            "column": "ERROR",
                            "matched_id": "",
                            "label": f"Excel parse error: {e}",
                            "row_index": ""
                        })
                        continue
                    if df.empty:
                        continue
                    scan_dataframe(fp, sheet, table_name, df, ftu_ids, records)
            else:
                # CSV/TSV
                sep = ','
                if fp.lower().endswith(".tsv"):
                    sep = '\t'
                try:
                    df = pd.read_csv(fp, dtype=str, sep=sep, engine='python', header=11)
                except Exception:
                    # fallback: try python engine without forcing sep
                    df = pd.read_csv(fp, dtype=str, engine='python', sep=None)
                if df.empty:
                    continue
                scan_dataframe(fp, None, table_name, df, ftu_ids, records)
        except Exception as exc:
            records.append({
                "input_file": fp,
                "sheet": None,
                "table_name": table_name,
                "column": "ERROR",
                "matched_id": "",
                "label": f"File-level error: {exc}",
                "row_index": ""
            })

    if not records:
        print("No matches found.")
        # write empty CSV for consistency
        out_dir = Path(out_csv).parent
        out_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(columns=["table_name","input_file","sheet","column","matched_id","label","row_index","count"]).to_csv(out_csv, index=False)
        return pd.DataFrame()

    df_records = pd.DataFrame(records)

    # compute counts: group by table/file/column/matched_id/label
    summary = (df_records
               .groupby(["table_name", "input_file", "sheet", "column", "matched_id", "label"], dropna=False)
               .agg(count=("matched_id","size"))
               .reset_index())
    # write output
    out_dir = Path(out_csv).parent
    out_dir.mkdir(parents=True, exist_ok=True)
    summary.to_csv(out_csv, index=False)
    print(f"Wrote summary CSV: {out_csv}")
    # print a quick per-file summary
    per_file = (summary.groupby(["table_name","input_file"])
                .agg(total_matches=("count","sum"), unique_ids=("matched_id","nunique"))
                .reset_index()
                .sort_values(["total_matches"], ascending=False))
    print("\nMatches per file (top 50):")
    print(per_file.head(50).to_string(index=False))
    return summary

def scan_dataframe(input_file: str, sheet: Optional[str], table_name: str, df: pd.DataFrame, ftu_ids: set, records: list):
    """
    Scan a single dataframe for matches and append to `records`.
    """
    columns = list(df.columns)
    # locate id columns (we will look for both possible ID columns)
    for id_candidate in ID_COLUMN_CANDIDATES:
        id_col = find_best_column(columns, [id_candidate])
        if not id_col:
            continue
        # determine corresponding label candidate for this ID column
        # map "EffectorLocation/ID" -> "EffectorLocation/LABEL", etc.
        corresponding_label_name = None
        # find matching label candidate that has same prefix before '/'
        if '/' in id_candidate:
            prefix = id_candidate.split('/', 1)[0]
            # try exact prefix + /LABEL
            test_label = prefix + "/LABEL"
            label_col = find_best_column(columns, [test_label])
            if label_col:
                corresponding_label_name = label_col
        # if not found, try any of the generic label candidates
        if not corresponding_label_name:
            label_col = find_best_column(columns, LABEL_COLUMN_CANDIDATES)
            if label_col:
                corresponding_label_name = label_col

        # iterate rows
        # convert entire column to string, preserving empties
        col_series = df[id_col].astype(str).fillna("").where(df[id_col].notna(), "")
        for idx, cell in col_series.items():
            ids_in_cell = split_ids_from_cell(cell)
            if not ids_in_cell:
                continue
            for found_id in ids_in_cell:
                if found_id in ftu_ids:
                    label_val = ""
                    if corresponding_label_name and corresponding_label_name in df.columns:
                        try:
                            raw = df.at[idx, corresponding_label_name]
                            label_val = "" if pd.isna(raw) else str(raw)
                        except Exception:
                            label_val = ""
                    records.append({
                        "input_file": input_file,
                        "sheet": sheet,
                        "table_name": table_name,
                        "column": id_col,
                        "matched_id": found_id,
                        "label": label_val,
                        "row_index": idx
                    })

# ---------------------------
# Main
# ---------------------------
def main():
    parser = argparse.ArgumentParser(description="Scan input tables for FTU UBERON IDs.")
    parser.add_argument("--input", "-i", default=INPUT_FOLDER, help="Input folder to search (recursive).")
    parser.add_argument("--out", "-o", default=OUT_CSV, help="Output CSV path for summary.")
    parser.add_argument("--no-recursive", action="store_true", help="Don't search subfolders.")
    args = parser.parse_args()

    print(f"Scanning folder: {args.input} (recursive={not args.no_recursive})")
    summary = scan_files(args.input, FTU_IDS, args.out, recursive=not args.no_recursive)
    if summary is None or summary.empty:
        print("No matches written.")
    else:
        print(f"Summary contains {len(summary)} grouped rows.")

if __name__ == "__main__":
    main()

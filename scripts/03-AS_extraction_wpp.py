#!/usr/bin/env python3
"""
Extract unique tissue effectors from CSV files.

New behavior (dedupe by ID):
- For rows where effector scale == "tissue" (case-insensitive),
  collects labels and IDs (from multiple possible columns).
- Excludes any ID that starts with "CL" (case-insensitive).
- Deduplicates by ID:
    - For each non-empty, non-CL ID, produce exactly one output row with AS_ID = ID
      and AS = all distinct labels that referenced that ID joined by " | ".
    - For labels that had no non-CL ID at all, produce one row per unique label
      with AS_ID empty string.
- Writes output CSV with columns AS, AS_ID (one row per unique ID plus unique-label-only rows).
"""

import os
import glob
import pandas as pd

input_folder = "./data/WPP Input Tables/"
output_tissue_file = "./output/analysis/AS_UBERON_in_WPP.csv"

EFFECTOR_SCALE_COLS = ["effector scale", "Effector Scale", "effector_scale", "EffectorScale"]
TISSUE_LABEL_COLS = [
    "Effector/LABEL", "Effector LABEL", "EffectorLabel", "Effector Label", "LABEL", "label",
    "EffectorLocation/LABEL", "EffectorLocation LABEL", "EffectorLocationLabel", "Effector Location Label",
    "EffectorLocation Label", "effectorlocationlabel", "AS"
]
TISSUE_ID_COLS = [
    "Effector/ID", "Effector ID", "EffectorID", "effector_id", "ID", "id", "AS_ID",
    "EffectorLocation/ID", "EffectorLocation ID", "EffectorLocationID", "effectorlocation_id", "effectorlocationid"
]

# -----------------------
# Helpers
# -----------------------
def find_all_columns(df, candidates):
    """Return list of matching column names from df (case-insensitive), preserving candidate order."""
    lowered = {c.lower(): c for c in df.columns}
    matches = []
    for cand in candidates:
        if cand in df.columns:
            matches.append(cand)
            continue
        lc = cand.lower()
        if lc in lowered:
            matches.append(lowered[lc])
    # unique-preserve-order
    seen = set()
    uniq = []
    for m in matches:
        if m not in seen:
            uniq.append(m)
            seen.add(m)
    return uniq

def clean_text(val):
    if pd.isna(val):
        return None
    s = str(val).strip()
    if s == "":
        return None
    return " ".join(s.split())

def split_ids_field(id_field, sep=";"):
    if pd.isna(id_field) or id_field is None:
        return []
    s = str(id_field).strip()
    if s == "":
        return []
    parts = [p.strip() for p in s.split(sep)]
    return [p for p in parts if p]

def is_cl_id(idstr):
    if idstr is None:
        return False
    return str(idstr).strip().upper().startswith("CL")

# -----------------------
# Main
# -----------------------
def collect_tissue_only_dedupe_by_id(input_folder, output_tissue_file):
    files = sorted(glob.glob(os.path.join(input_folder, "*.csv")))
    if not files:
        print(f"No CSV files found in: {input_folder}")
        return

    # Map from id -> set(labels)
    id_to_labels = {}
    # Set of labels that were seen but had no non-CL ID anywhere
    labels_with_no_id = set()
    per_file_counts = {}

    for fp in files:
        fname = os.path.basename(fp)
        fname_l = fname.lower()
        header_row = 12 if "endocrine" in fname_l else 11

        try:
            df = pd.read_csv(fp, dtype=str, header=header_row)
        except Exception as e:
            # try utf-8-sig fallback then skip
            try:
                df = pd.read_csv(fp, dtype=str, header=header_row, encoding="utf-8-sig")
            except Exception:
                print(f"[ERROR] Could not read {fname}: {e} -- skipping.")
                per_file_counts[fname] = 0
                continue

        esc_cols = find_all_columns(df, EFFECTOR_SCALE_COLS)
        esc_col = esc_cols[0] if esc_cols else None
        label_cols = find_all_columns(df, TISSUE_LABEL_COLS)
        id_cols = find_all_columns(df, TISSUE_ID_COLS)

        if esc_col is None:
            print(f"[WARN] File {fname} has no 'effector scale' column. Skipping file.")
            per_file_counts[fname] = 0
            continue

        esc_series = df[esc_col].astype(str).str.strip().str.lower()
        tissue_mask = esc_series == "tissue"
        tissue_count = 0

        if tissue_mask.any():
            if not label_cols:
                print(f"[WARN] {fname} has tissue rows but no tissue label column found; tissue rows ignored.")
            else:
                for _, row in df.loc[tissue_mask].iterrows():
                    tissue_count += 1
                    # collect labels in this row
                    labels_found = []
                    for col in label_cols:
                        lbl = clean_text(row.get(col))
                        if lbl:
                            labels_found.append(lbl)

                    if not labels_found:
                        continue  # no label -> skip row

                    # collect non-CL ids in this row, splitting multi-ids
                    ids_found = []
                    for idcol in id_cols:
                        raw_field = row.get(idcol)
                        parts = split_ids_field(raw_field, sep=";")
                        for p in parts:
                            pclean = clean_text(p)
                            if not pclean:
                                continue
                            if is_cl_id(pclean):
                                continue
                            ids_found.append(pclean)

                    if ids_found:
                        # for every non-CL id, add association to labels
                        for idv in ids_found:
                            if idv not in id_to_labels:
                                id_to_labels[idv] = set()
                            for lbl in labels_found:
                                id_to_labels[idv].add(lbl)
                    else:
                        # record labels that currently have no non-CL id
                        for lbl in labels_found:
                            labels_with_no_id.add(lbl)

        per_file_counts[fname] = tissue_count

    # Now build output rows:
    # - one row per unique non-empty ID with labels joined by " | "
    # - plus one row per unique label in labels_with_no_id with empty AS_ID (unless that label also appears attached to some ID; in that case skip)
    rows = []

    # sort IDs for stable output
    for idv in sorted(id_to_labels, key=lambda x: x):
        labels = sorted(id_to_labels[idv])
        # join labels with ' | ' so we retain all label text
        as_field = " | ".join(labels)
        rows.append({"AS": as_field, "AS_ID": idv})

    # For labels that had no ID AND are not present in any id_to_labels value (avoid duplicates)
    labels_in_ids = set(l for labels in id_to_labels.values() for l in labels)
    leftover_labels = sorted(lbl for lbl in labels_with_no_id if lbl not in labels_in_ids)
    for lbl in leftover_labels:
        rows.append({"AS": lbl, "AS_ID": ""})

    # Build DataFrame and write
    out_df = pd.DataFrame(rows, columns=["AS", "AS_ID"])
    os.makedirs(os.path.dirname(output_tissue_file) or ".", exist_ok=True)
    out_df.to_csv(output_tissue_file, index=False)

    # Summary
    total_tissue_rows = sum(per_file_counts.values())
    print("\n=== Summary ===")
    print(f"Files scanned: {len(files)}")
    for fn, ct in per_file_counts.items():
        print(f"  {fn}: tissue_matches={ct}")
    print(f"Total tissue-matched rows: {total_tissue_rows}")
    print(f"Unique IDs output rows: {len(out_df[out_df['AS_ID'] != ''])}")
    print(f"Label-only output rows (no ID): {len(out_df[out_df['AS_ID'] == ''])}")
    print(f"Total output rows: {len(out_df)} -> saved to: {output_tissue_file}")

if __name__ == "__main__":
    collect_tissue_only_dedupe_by_id(input_folder, output_tissue_file)

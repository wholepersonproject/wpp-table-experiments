################### ASCTB Matched########################


# Compare tissue IDs (from WPP tissue output) against ASTCB master list.


# import os
# import pandas as pd

# # -----------------------
# # USER CONFIG
# # -----------------------
# tissue_output_file = "./output/analysis/AS_UBERON_in_WPP.csv"        # your WPP tissue file
# astcb_master_file = "./data/all_asctb_ids_and_types.csv"                    # ASTCB master file
# output_missing_file = "./output/analysis/AS_ids_missing_in_asctb.csv"
# output_present_file = "./output/analysis/as_ids_present_in_astcb.csv"
# filtered_cl_output = "./output/analysis/tissue_ids_filtered_out_CL.csv"

# ID_SEPARATOR = ";"

# ASTCB_ID_COL_CANDIDATES = ["id"]

# # -----------------------
# # Helpers
# # -----------------------
# def find_column(df, candidates):
#     """Return first matching column name from df (case-insensitive)."""
#     lowered = {c.lower(): c for c in df.columns}
#     for cand in candidates:
#         if cand in df.columns:
#             return cand
#         if cand.lower() in lowered:
#             return lowered[cand.lower()]
#     return None

# def split_ids_field(id_field, sep=ID_SEPARATOR):
#     """Split and normalize semicolon-separated IDs."""
#     if pd.isna(id_field) or id_field is None or str(id_field).strip() == "":
#         return []
#     parts = [p.strip() for p in str(id_field).split(sep)]
#     return [p for p in parts if p]

# def is_cl_id(idstr):
#     return idstr and str(idstr).strip().upper().startswith("CL")

# # -----------------------
# # Main logic
# # -----------------------
# def main():
#     # ---- Load tissue output ----
#     if not os.path.exists(tissue_output_file):
#         print(f"[ERROR] Tissue file not found: {tissue_output_file}")
#         return
#     tissue_df = pd.read_csv(tissue_output_file, dtype=str)

#     id_col = "EffectorID" if "EffectorID" in tissue_df.columns else next((c for c in tissue_df.columns if "id" in c.lower()), None)
#     if id_col is None:
#         print("[ERROR] No ID column found in tissue file.")
#         return

#     # Build mapping: ID -> set(WPP labels)
#     id_to_labels = {}
#     cl_ids_set = set()
#     total_id_occurrences = 0

#     for _, row in tissue_df.iterrows():
#         label = row.get("EffectorLabel") or ""
#         ids = split_ids_field(row.get(id_col))
#         for i in ids:
#             total_id_occurrences += 1
#             if is_cl_id(i):
#                 cl_ids_set.add(i)
#                 continue
#             id_to_labels.setdefault(i, set()).add(label)

#     print(f"Unique non-CL tissue IDs: {len(id_to_labels)}")
#     print(f"Filtered out CL IDs: {len(cl_ids_set)}")

#     # Save filtered CL IDs for inspection
#     if cl_ids_set:
#         pd.DataFrame(sorted(cl_ids_set), columns=["CL_ID"]).to_csv(filtered_cl_output, index=False)
#         print(f"Saved filtered CL IDs to: {filtered_cl_output}")

#     # ---- Load ASTCB master ----
#     if not os.path.exists(astcb_master_file):
#         print(f"[ERROR] ASTCB master file not found: {astcb_master_file}")
#         return
#     astcb_df = pd.read_csv(astcb_master_file, dtype=str)

#     astcb_id_col = find_column(astcb_df, ASTCB_ID_COL_CANDIDATES)
#     if astcb_id_col is None:
#         astcb_id_col = next((c for c in astcb_df.columns if "id" in c.lower()), None)
#         if astcb_id_col is None:
#             print("[ERROR] Could not detect ID column in ASTCB file.")
#             return
#         print(f"[INFO] Using guessed ASTCB ID column: {astcb_id_col}")

#     astcb_ids = {str(v).strip() for v in astcb_df[astcb_id_col].dropna().astype(str) if str(v).strip()}
#     print(f"Total unique IDs in ASTCB ({astcb_id_col}): {len(astcb_ids)}")

#     # ---- Compare ----
#     tissue_ids = set(id_to_labels.keys())
#     present_ids = sorted(tissue_ids & astcb_ids)
#     missing_ids = sorted(tissue_ids - astcb_ids)

#     # ---- Save Missing IDs ----
#     missing_rows = [{"MissingID": mid, "WPP_ReferencingLabels": ";".join(sorted(id_to_labels[mid]))} for mid in missing_ids]
#     if missing_rows:
#         pd.DataFrame(missing_rows).to_csv(output_missing_file, index=False)
#         print(f"Saved {len(missing_rows)} missing IDs to: {output_missing_file}")
#     else:
#         print("No missing tissue IDs — all found in ASTCB.")

#     # ---- Save Present IDs ----
#     present_rows = [{"PresentID": pid, "WPP_ReferencingLabels": ";".join(sorted(id_to_labels[pid]))} for pid in present_ids]
#     if present_rows:
#         pd.DataFrame(present_rows).to_csv(output_present_file, index=False)
#         print(f"Saved {len(present_rows)} present IDs to: {output_present_file}")
#     else:
#         print("No tissue IDs matched ASTCB entries.")

#     # ---- Summary ----
#     print("\n=== Summary ===")
#     print(f"Total WPP tissue IDs (non-CL): {len(tissue_ids)}")
#     print(f"Present in ASTCB: {len(present_ids)}")
#     print(f"Missing from ASTCB: {len(missing_ids)}")
#     print(f"Filtered out CL IDs: {len(cl_ids_set)}")

# if __name__ == "__main__":
#     main()
#!/usr/bin/env python3
"""
Collect Uberon IDs from WPP CSVs (EffectorLocation/ID and Effector/ID for tissue rows),
normalize them to canonical form (UBERON:0002040), then compare against ASTCB/HRA master
counting ONLY true Uberon IDs.

Outputs:
 - ./output/analysis/all_Uberon_statistics/all_Uberon_ids_in_WPP.csv  (column Uberon_ID)
 - ./output/analysis/all_Uberon_statistics/uberon_ids_missing_in_asctb.csv
 - ./output/analysis/all_Uberon_statistics/uberon_ids_present_in_astcb.csv

Final printed stats (exact format requested):
- Total Uberon ids in WPP => ...
- WPP intersection HRA => ...
- Only in WPP => ...
- Total in HRA => ...
- Only in HRA => ...
"""

import os
import glob
import re
import pandas as pd

input_folder = "./data/WPP Input Tables/"
tissue_output_file = "./output/analysis/all_Uberon_statistics/all_Uberon_ids_in_WPP.csv"
astcb_master_file = "./data/all_asctb_ids_and_types.csv"
output_missing_file = "./output/analysis/all_Uberon_statistics/uberon_ids_missing_in_asctb.csv"
output_present_file = "./output/analysis/all_Uberon_statistics/uberon_ids_present_in_astcb.csv"

ID_SEPARATOR = ";"

# candidate columns
EFFECTOR_SCALE_COLS = ["effector scale", "Effector Scale", "effector_scale", "EffectorScale"]
TISSUE_LABEL_COLS = ["Effector/LABEL", "Effector LABEL", "EffectorLabel", "Effector Label", "LABEL", "label", "AS"]
EFFLOC_ID_CANDIDATES = ["EffectorLocation/ID", "EffectorLocation ID", "effectorlocation/id", "effectorlocation_id"]
EFF_ID_CANDIDATES = ["Effector/ID", "Effector ID", "EffectorID", "effector_id", "ID", "id", "AS_ID"]
ASTCB_ID_COL_CANDIDATES = ["id", "ID", "uberon_id", "Uberon", "Uberon ID"]

def find_column(df, candidates):
    lowered = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand in df.columns:
            return cand
        lc = cand.lower()
        if lc in lowered:
            return lowered[lc]
    return None

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

_uberon_digits_re = re.compile(r"(\d+)")
def normalize_to_uberon(idstr):
    """
    If idstr contains a numeric part corresponding to a UBERON id, return canonical
    'UBERON:0002040' (7-digit zero-padded). Otherwise return None.

    Examples handled:
      - 'UBERON:0002040' -> 'UBERON:0002040'
      - 'uberon:0002040' -> 'UBERON:0002040'
      - '0002040' -> 'UBERON:0002040'
      - 'UBERON_2040' -> 'UBERON:0002040'
      - 'FMA:12345' -> None (we do not accept non-Uberon prefixes)
      - 'some-text' -> None
    """
    if idstr is None:
        return None
    s = str(idstr).strip()
    if s == "":
        return None

    s_upper = s.upper()

    # Quick accept if it already looks like UBERON:<digits>
    m = re.match(r"^UBERON[:_]?0*([0-9]+)$", s_upper)
    if m:
        num = int(m.group(1))
        return f"UBERON:{num:07d}"

    # If it starts with a different ontology prefix (e.g., 'FMA:', 'CL:'), reject
    if re.match(r"^[A-Z]+[:_].*$", s_upper):
        prefix = s_upper.split(":", 1)[0].split("_", 1)[0]
        if prefix != "UBERON":
            return None
        # if prefix was UBERON but didn't match earlier, continue below

    # fallback: find the first long-ish digit sequence and treat it as Uberon id
    md = _uberon_digits_re.search(s)
    if not md:
        return None
    num = int(md.group(1))
    # apply heuristic: Uberon numeric ids are typically at least 4 digits; we accept if numeric length >= 4
    if len(md.group(1)) < 4:
        return None
    return f"UBERON:{num:07d}"

def try_read_csv(fp, header_row):
    tries = [
        {"sep": None, "encoding": None},
        {"sep": ";", "encoding": None},
        {"sep": None, "encoding": "utf-8-sig"},
        {"sep": ";", "encoding": "utf-8-sig"},
    ]
    last_exc = None
    for t in tries:
        kwargs = {"dtype": str, "header": header_row}
        if t["sep"] is not None:
            kwargs["sep"] = t["sep"]
        if t["encoding"] is not None:
            kwargs["encoding"] = t["encoding"]
        try:
            return pd.read_csv(fp, **kwargs)
        except Exception as e:
            last_exc = e
    raise last_exc

def collect_unique_uberon_ids_from_wpp(input_folder, output_file):
    files = sorted(glob.glob(os.path.join(input_folder, "*.csv")))
    if not files:
        print(f"[ERROR] No CSV files found in: {input_folder}")
        return False

    uberon_ids = set()
    cl_ids = set()
    per_file_counts = {}
    total_rows_scanned = 0
    non_uberon_ids_seen = set()

    for fp in files:
        fname = os.path.basename(fp)
        fname_l = fname.lower()
        header_row = 12 if "endocrine" in fname_l else 11

        try:
            df = try_read_csv(fp, header_row=header_row)
        except Exception as e:
            print(f"[WARN] Could not read {fname}: {e} -- skipping file.")
            per_file_counts[fname] = 0
            continue

        esc_col = find_column(df, EFFECTOR_SCALE_COLS)
        if esc_col is None:
            per_file_counts[fname] = 0
            print(f"[WARN] File {fname} has no 'effector scale' column. Skipping.")
            continue

        effloc_col = find_column(df, EFFLOC_ID_CANDIDATES)
        eff_col = find_column(df, EFF_ID_CANDIDATES)

        esc_series = df[esc_col].astype(str).str.strip().str.lower()
        tissue_mask = esc_series == "tissue"
        tissue_count = 0

        if tissue_mask.any():
            for _, row in df.loc[tissue_mask].iterrows():
                total_rows_scanned += 1
                tissue_count += 1
                ids = []
                if effloc_col:
                    ids.extend(split_ids_field(row.get(effloc_col)))
                if eff_col:
                    ids.extend(split_ids_field(row.get(eff_col)))
                for raw in ids:
                    if raw is None:
                        continue
                    raw = raw.strip()
                    if raw == "":
                        continue
                    if is_cl_id(raw):
                        cl_ids.add(raw)
                        continue
                    norm = normalize_to_uberon(raw)
                    if norm:
                        uberon_ids.add(norm)
                    else:
                        non_uberon_ids_seen.add(raw)

        per_file_counts[fname] = tissue_count

    # write unique Uberon IDs to CSV
    os.makedirs(os.path.dirname(output_file) or ".", exist_ok=True)
    pd.DataFrame(sorted(uberon_ids), columns=["Uberon_ID"]).to_csv(output_file, index=False)

    print("\n=== Uberon collection summary ===")
    print(f"Files scanned: {len(files)}")
    for fn, ct in per_file_counts.items():
        print(f"  {fn}: tissue_rows={ct}")
    print(f"Total tissue rows scanned (approx): {total_rows_scanned}")
    print(f"Unique canonical Uberon IDs written: {len(uberon_ids)} -> {output_file}")
    print(f"Filtered CL IDs: {len(cl_ids)}")
    print(f"Non-Uberon ID strings seen (sample up to 10): {list(sorted(non_uberon_ids_seen))[:10]}")
    print(f"Total distinct non-Uberon ID strings seen in tissue rows: {len(non_uberon_ids_seen)}")

    return True

def compare_to_astcb(tissue_ids_file, astcb_master_file, out_missing, out_present):
    if not os.path.exists(tissue_ids_file):
        print(f"[ERROR] Tissue file not found: {tissue_ids_file}")
        return

    tissue_df = pd.read_csv(tissue_ids_file, dtype=str)
    # tissue_df has single column "Uberon_ID"
    id_col = "Uberon_ID" if "Uberon_ID" in tissue_df.columns else next((c for c in tissue_df.columns if "id" in c.lower()), None)
    if id_col is None:
        print("[ERROR] No ID column found in tissue file.")
        return

    # Build set of tissue IDs (already unique) and ensure canonical form
    tissue_ids_raw = {str(v).strip() for v in tissue_df[id_col].dropna().astype(str) if str(v).strip()}
    tissue_ids = set()
    for v in tissue_ids_raw:
        norm = normalize_to_uberon(v)
        if norm:
            tissue_ids.add(norm)

    print(f"Unique canonical Uberon IDs in WPP: {len(tissue_ids)}")

    # Load ASTCB master
    if not os.path.exists(astcb_master_file):
        print(f"[ERROR] ASTCB master file not found: {astcb_master_file}")
        return
    astcb_df = pd.read_csv(astcb_master_file, dtype=str)

    astcb_id_col = find_column(astcb_df, ASTCB_ID_COL_CANDIDATES)
    if astcb_id_col is None:
        astcb_id_col = next((c for c in astcb_df.columns if "id" in c.lower()), None)
        if astcb_id_col is None:
            print("[ERROR] Could not detect ID column in ASTCB file.")
            return
        print(f"[INFO] Using guessed ASTCB ID column: {astcb_id_col}")

    astcb_raw_ids = [str(v).strip() for v in astcb_df[astcb_id_col].dropna().astype(str) if str(v).strip()]
    total_astcb_raw = len(astcb_raw_ids)
    # normalize ASTCB IDs to canonical Uberon when possible
    astcb_uberon_set = set()
    astcb_non_uberon = set()
    for r in astcb_raw_ids:
        if is_cl_id(r):
            continue
        norm = normalize_to_uberon(r)
        if norm:
            astcb_uberon_set.add(norm)
        else:
            astcb_non_uberon.add(r)

    print(f"Total entries in ASTCB master (raw rows read): {total_astcb_raw}")
    print(f"Total canonical Uberon IDs found in ASTCB: {len(astcb_uberon_set)}")
    print(f"Total non-Uberon (filtered out) in ASTCB: {len(astcb_non_uberon)} (sample up to 10): {list(sorted(astcb_non_uberon))[:10]}")

    # comparisons
    present_ids = sorted(tissue_ids & astcb_uberon_set)
    missing_ids = sorted(tissue_ids - astcb_uberon_set)

    # Save Missing and Present (IDs only)
    if missing_ids:
        pd.DataFrame({"Missing_AS_ID": missing_ids}).to_csv(out_missing, index=False)
        print(f"Saved {len(missing_ids)} missing IDs to: {out_missing}")
    else:
        print("No missing tissue IDs — all found in ASTCB (Uberon-only).")

    if present_ids:
        pd.DataFrame({"Present_AS_ID": present_ids}).to_csv(out_present, index=False)
        print(f"Saved {len(present_ids)} present IDs to: {out_present}")
    else:
        print("No tissue IDs matched ASTCB entries (Uberon-only).")

    # final stats requested (Uberon-only)
    total_uberon_in_wpp = len(tissue_ids)
    wpp_intersection_hra = len(tissue_ids & astcb_uberon_set)
    only_in_wpp = len(tissue_ids - astcb_uberon_set)
    total_in_hra = len(astcb_uberon_set)
    only_in_hra = len(astcb_uberon_set - tissue_ids)

    print("\n=== Final counts ===")
    print(f"- Total Uberon ids in WPP => {total_uberon_in_wpp}")
    print(f"- WPP intersection HRA => {wpp_intersection_hra}")
    print(f"- Only in WPP => {only_in_wpp}")
    print(f"- Total in HRA => {total_in_hra}")
    print(f"- Only in HRA => {only_in_hra}")

# -----------------------
# Main
# -----------------------
def main():
    ok = collect_unique_uberon_ids_from_wpp(input_folder, tissue_output_file)
    if not ok:
        return
    compare_to_astcb(tissue_output_file, astcb_master_file, output_missing_file, output_present_file)

if __name__ == "__main__":
    main()

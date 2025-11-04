import pandas as pd
import re

# --- Step 1: Load your main organ CSV ---
main = pd.read_csv("data\Female-reproductive-system_v1.0_DRAFT - Sheet1.csv", header=11, encoding="utf-8-sig")
main.columns = main.columns.str.strip()

# --- Step 2: Load ASCT+B master reference table ---
# Columns: organ, id, cf_asctb_type, label
asctb = pd.read_csv("data/all_asctb_ids_and_types.csv", encoding="utf-8-sig")
asctb.columns = asctb.columns.str.strip()

# Normalize IDs for comparison
asctb["id_norm"] = asctb["id"].astype(str).str.strip().str.upper()
asctb_ids = set(asctb["id_norm"])

# --- Step 3: Find all /LABEL and /ID columns dynamically ---
label_cols = [c for c in main.columns if re.search(r"/\d+/LABEL", c, re.IGNORECASE)]
id_cols    = [c for c in main.columns if re.search(r"/\d+/ID", c, re.IGNORECASE)]

def extract_index(c):
    m = re.search(r"/(\d+)/", c)
    return int(m.group(1)) if m else 0

# Sort descending → start from deepest level
label_cols = sorted(label_cols, key=extract_index, reverse=True)
id_cols    = sorted(id_cols, key=extract_index, reverse=True)

# --- Step 4: Extract lowest non-empty label–ID pair per row ---
def get_lowest_non_empty(row):
    for l_col, i_col in zip(label_cols, id_cols):
        label = str(row.get(l_col, "")).strip()
        id_ = str(row.get(i_col, "")).strip().upper()
        if label and id_ and id_.lower() not in ["nan", "none", ""]:
            return label, id_
    return None, None

main[["Label", "ID"]] = main.apply(lambda r: pd.Series(get_lowest_non_empty(r)), axis=1)

# --- Step 5: Check if found in ASCTB ---
main["Found_in_ASCTB"] = main["ID"].apply(
    lambda x: "Found" if isinstance(x, str) and x.strip().upper() in asctb_ids else "Not found"
)

# --- Step 6: Replace blanks with "Unknown" ---
main["Label"] = main["Label"].fillna("Unknown")
main["ID"] = main["ID"].fillna("Unknown")

# --- Step 7: Output final minimal table ---
final = main[["Label", "ID", "Found_in_ASCTB"]]

final.to_csv("data/lowest_missing_female_reproductive_data.csv", index=False, encoding="utf-8-sig")
print(final.head(10))
print(f"\nTotal rows: {len(final)} | Found: {(final['Found_in_ASCTB'] == 'Found').sum()} | Not found: {(final['Found_in_ASCTB'] == 'Not found').sum()}")
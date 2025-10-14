import pandas as pd
import re

# Load all CSVs
main = pd.read_csv("data/Urinary-system_v1.0_DRAFT_250923-urinary-system_v1.0_DRAFT.csv", header=11)  # change header index if needed
file2 = pd.read_csv("data/CT_ids.csv")
file3 = pd.read_csv("data/UBERON_ids.csv")

# Collect all ID columns in main.csv that match "Structure/x/ID" or "Cell/x/ID"
id_cols = [
    col for col in main.columns
    if re.search(r"(Structure|Cell)/\d+/ID", col, re.IGNORECASE)
]

print("Detected ID columns:", id_cols)

# Flatten all IDs from these columns into a single list
if id_cols:
    main_ids = pd.unique(main[id_cols].values.ravel())
else:
    main_ids = []

# Combine IDs from file2 and file3 (assuming column name contains 'id')
id_cols_2 = [col for col in file2.columns if 'id' in col.lower()]
id_cols_3 = [col for col in file3.columns if 'id' in col.lower()]

file2_ids = pd.unique(file2[id_cols_2].values.ravel()) if id_cols_2 else []
file3_ids = pd.unique(file3[id_cols_3].values.ravel()) if id_cols_3 else []

# Combine both
all_other_ids = set(file2_ids) | set(file3_ids)

# Filter out NaN values
main_ids = {str(x).strip() for x in main_ids if pd.notna(x)}
all_other_ids = {str(x).strip() for x in all_other_ids if pd.notna(x)}

# Find missing IDs
missing_ids = sorted(main_ids - all_other_ids)

print(f"Total Structure/Cell IDs in main.csv: {len(main_ids)}")
print(f"Missing IDs (not in file2.csv or file3.csv): {len(missing_ids)}")
print("\n".join(missing_ids))

pd.DataFrame({"missing_id": missing_ids}).to_csv("data/missing_ids.csv", index=False)
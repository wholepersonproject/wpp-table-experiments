import os
import glob
import pandas as pd

# --- CONFIGURATION ---
INPUT_DIR = "./data/WPP Tables"  # 👈 change this to your directory path
ID_COLUMN = "EffectorLocation/ID"        # The column name to check

# --- FTU IDs ---
ftu_ids = {
    "UBERON:0004203", "UBERON:0001289", "UBERON:0004205", "UBERON:0004193",
    "UBERON:0001285", "UBERON:0004204", "UBERON:0001229", "UBERON:0001291",
    "UBERON:0004647", "UBERON:0002299", "UBERON:8410043", "UBERON:0000006",
    "UBERON:0001263", "UBERON:0014725", "UBERON:0004179", "UBERON:0001983",
    "UBERON:0000412", "UBERON:0002073", "UBERON:0013487", "UBERON:0001213",
    "UBERON:0001250", "UBERON:0001959", "UBERON:0002125", "UBERON:0001831",
    "UBERON:0001832", "UBERON:0001736",
}

# --- MAIN SCRIPT ---
def count_ftu_entries_in_csvs(input_dir: str):
    csv_files = glob.glob(os.path.join(input_dir, "*.csv"))
    if not csv_files:
        print(f"⚠️ No CSV files found in directory: {input_dir}")
        return

    print(f"📂 Found {len(csv_files)} CSV files in {input_dir}")
    print("--------------------------------------------------")

    summary = []

    for file_path in csv_files:
        try:
            df = pd.read_csv(file_path, encoding="utf-8-sig", low_memory=False)
        except Exception as e:
            print(f"❌ Error reading {os.path.basename(file_path)}: {e}")
            continue

        # Check if the column exists
        if ID_COLUMN not in df.columns:
            print(f"⚠️ '{ID_COLUMN}' not found in {os.path.basename(file_path)}")
            summary.append((os.path.basename(file_path), 0, len(df)))
            continue

        # Normalize the column to string & strip whitespace
        df[ID_COLUMN] = df[ID_COLUMN].astype(str).str.strip()

        # Count matches
        count = df[ID_COLUMN].isin(ftu_ids).sum()
        total = len(df)

        print(f"✅ {os.path.basename(file_path)} — {count} FTU entries out of {total} rows")

        summary.append((os.path.basename(file_path), count, total))

    # --- Summary table ---
    print("\n📊 Summary:")
    print("File Name".ljust(40), "FTU Count".rjust(12), "Total Rows".rjust(12))
    print("-" * 70)
    for fname, count, total in summary:
        print(f"{fname.ljust(40)} {str(count).rjust(12)} {str(total).rjust(12)}")

# --- RUN ---
if __name__ == "__main__":
    count_ftu_entries_in_csvs(INPUT_DIR)

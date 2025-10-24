import pandas as pd

input_path = "data/sxt_loweststruct_table.csv"

df = pd.read_csv(input_path, encoding="utf-8-sig")

unique_structures = (
    df["Lowest Structure"]
    .dropna()        # remove NaN values
    .astype(str)     # ensure all are strings
    .unique()        # get unique values
)

unique_df = pd.DataFrame(unique_structures, columns=["Lowest Structure"])

output_path = "data/unique_lowest_structures.csv"
unique_df.to_csv(output_path, index=False, encoding="utf-8-sig")

print(f"Extracted unique structures")

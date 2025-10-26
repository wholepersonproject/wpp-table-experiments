# import pandas as pd
# import matplotlib.pyplot as plt

# # Load the CSV
# df = pd.read_csv("./output/lowest_type.csv")

# # Map lowest type to spatial scale (optional)
# scale_map = {
#     "organ": 0.1,
#     "FTU": 1e-4,
#     "CT": 1e-5,
#     "B": 1e-8
# }
# df["space_m"] = df["Lowest Type"].map(scale_map)

# # Melt all time columns into long form
# melted = df.melt(
#     id_vars=["Lowest Type", "space_m"],
#     var_name="Time Range",
#     value_name="Value"
# )

# # Group duplicates: same Lowest Type + Time Range → join entries
# grouped = (
#     melted.groupby(["Time Range", "Lowest Type"])["Value"]
#     .apply(lambda x: "\n".join(x.dropna().astype(str).unique()))
#     .unstack(fill_value="")
# )

# # Reorder time ranges
# time_order = [
#     "<1 second",
#     "1s - 1min",
#     "1min - 1hr",
#     "1hr - 1day",
#     "1day - 1week",
#     "1 week - 1 year",
#     "1 year or longer"
# ]
# grouped = grouped.reindex(time_order)

# # Display as a readable table
# fig, ax = plt.subplots(figsize=(10, 6))
# ax.set_axis_off()
# tbl = ax.table(
#     cellText=grouped.values,
#     rowLabels=grouped.index,
#     colLabels=grouped.columns,
#     cellLoc="center",
#     loc="center"
# )

# tbl.auto_set_font_size(False)
# tbl.set_fontsize(7.5)
# tbl.scale(1.2, 1.4)

# plt.title("Temporal vs Spatial Scale Table", pad=20)
# plt.show()

import pandas as pd

df = pd.read_csv("./output/lowest_type.csv")

#Temporary mapping not ideal
scale_map = {
    "organ": "10 cm",
    "FTU": "100 µm",
    "CT": "10 µm",
    "B": "10 nm"
}
df["space_label"] = df["Lowest Type"].map(scale_map)

# Melt all time columns into one
melted = df.melt(
    id_vars=["Lowest Type", "space_label"],
    var_name="Time Range",
    value_name="Value"
)

# Group duplicates: same Lowest Type + Time Range → combine all values
grouped = (
    melted.groupby(["Time Range", "Lowest Type"])["Value"]
    .apply(lambda x: "; ".join(x.dropna().astype(str).unique()))
    .unstack(fill_value="")
)

time_order = [
    "<1 second",
    "1s - 1min",
    "1min - 1hr",
    "1hr - 1day",
    "1day - 1week",
    "1 week - 1 year",
    "1 year or longer"
]
grouped = grouped.reindex(time_order)

# Save to CSV
output_path = "./output/temporal_spatial_table.csv"
grouped.to_csv(output_path, index=True, encoding="utf-8-sig")

print(f"Saved combined table to: {output_path}")

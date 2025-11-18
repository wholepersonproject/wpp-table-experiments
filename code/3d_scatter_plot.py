import pandas as pd
import glob
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import numpy as np
import os
import re

# ----------------------------------------------------
# 1. Load ALL organ-system CSVs from the output folder
# ----------------------------------------------------
input_folder = "./output/temporal_spatial_output/v3/"
output_folder = "./output/3d_scatter_plots/"
os.makedirs(output_folder, exist_ok=True)
files = glob.glob(os.path.join(input_folder, "*.csv"))

# ----------------------------------------------------
# 2. Shared configuration
# ----------------------------------------------------
spatial_order = ["Organ", "AS", "FTU", "CT", "B"]
time_order = [
    "<1 second", "1s - < 1min", "1min - < 1hr", "1hr - < 1day",
    "1day - < 1week", "1 week - < 1 year", "1 year or longer"
]

def process_count(x):
    if pd.isna(x) or x.strip() == "":
        return 0
    return len([p for p in x.split(";") if p.strip()])

def extract_organ_system_name(filename):
    """
    Extract first two words from filename (before '_final...' or '.csv').
    e.g. 'male_reproductive_system_xyz.csv' → 'male reproductive'
    """
    base = os.path.basename(filename).replace("_final_spatial_temporal_v3.csv", "").replace(".csv", "")
    words = base.split("_")
    # join first two words if they exist
    name = " ".join(words[:2]) if len(words) >= 2 else words[0]
    return name

# Collect all organ system names for consistent z-axis
organ_system_names = [extract_organ_system_name(f) for f in files]
organ_system_order = sorted(set(organ_system_names))  # consistent ordering

# ----------------------------------------------------
# 3. Loop through each file and generate individual plots
# ----------------------------------------------------
for f in files:
    organ_system_label = extract_organ_system_name(f)
    print(f"Processing {organ_system_label}...")

    df = pd.read_csv(f)

    # --- Count processes ---
    for col in ["Organ", "AS", "FTU", "CT", "B"]:
        df[col + "_count"] = df[col].apply(process_count)

    # --- Melt to long form ---
    long_df = df.melt(
        id_vars=["Time Range"],
        value_vars=["Organ_count", "AS_count", "FTU_count", "CT_count", "B_count"],
        var_name="Spatial Scale",
        value_name="Count"
    )

    long_df["Spatial Scale"] = long_df["Spatial Scale"].str.replace("_count", "")
    long_df["Organ System"] = organ_system_label
    long_df = long_df[long_df["Count"] > 0]

    # --- Encode categorical axes ---
    long_df["z"] = long_df["Organ System"].astype("category").cat.set_categories(organ_system_order).cat.codes
    long_df["x"] = long_df["Spatial Scale"].astype("category").cat.set_categories(spatial_order).cat.codes
    long_df["y"] = long_df["Time Range"].astype("category").cat.set_categories(time_order).cat.codes

    # ----------------------------------------------------
    # 4. Plot 3D scatter for this organ system
    # ----------------------------------------------------
    # ----------------------------------------------------
    # 4. Plot 3D scatter for this organ system
    # ----------------------------------------------------
    fig = plt.figure(figsize=(18, 14))
    ax = fig.add_subplot(111, projection="3d")

    xs = long_df["x"]
    ys = long_df["y"]
    zs = long_df["z"]
    sizes = long_df["Count"] * 40
    colors = long_df["Count"]

    p = ax.scatter(xs, ys, zs, s=sizes, c=colors, cmap="YlGnBu", alpha=0.9)

    # --- Axis setup ---
    ax.set_xticks(range(len(spatial_order)))
    ax.set_xticklabels(spatial_order, rotation=45, ha="right", fontsize=11)
    ax.set_xlabel("Spatial Scale", fontsize=13, labelpad=25)

    ax.set_yticks(range(len(time_order)))
    ax.set_yticklabels(time_order, rotation=15, fontsize=11)
    ax.set_ylabel("Time Scale", fontsize=13, labelpad=25)

    ax.set_zticks(range(len(organ_system_order)))
    ax.set_zticklabels(organ_system_order, fontsize=11)
    ax.set_zlabel("Organ System", fontsize=13, labelpad=35)

    # Expand plot space so end labels aren’t cut off
    ax.set_xlim(-0.5, len(spatial_order)-0.5)
    ax.set_ylim(-0.5, len(time_order)-0.5)
    ax.set_zlim(-0.5, len(organ_system_order)-0.5)

    # --- Title & colorbar ---
    ax.set_title(f"Temporal–Spatial Distribution — {organ_system_label.title()}",
                fontsize=16, pad=30)

    cbar = fig.colorbar(p, ax=ax, shrink=0.5, pad=0.1)
    cbar.set_label("Number of Processes", rotation=270, labelpad=25, fontsize=12)

    plt.subplots_adjust(left=0.15, right=0.9, bottom=0.15, top=0.9)
    plt.tight_layout()


    # --- Save output ---
    safe_name = organ_system_label.replace(" ", "_")
    output_path = os.path.join(output_folder, f"{safe_name}_3D_scatter.png")
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close(fig)

    print(f"Saved: {output_path}")

print("\nAll plots generated successfully.")

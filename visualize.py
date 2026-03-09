"""
Generate data quality comparison visualizations for the portfolio README.
Creates before/after charts showing the impact of the cleaning pipeline.
"""

import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import os
import json

# Paths
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
RAW_PATH = os.path.join(BASE_DIR, "data", "raw", "messy_sales_data.csv")
CLEAN_PATH = os.path.join(BASE_DIR, "data", "cleaned", "clean_sales_data.csv")
REPORT_PATH = os.path.join(BASE_DIR, "data", "cleaned", "quality_report.json")
SCREENSHOTS_DIR = os.path.join(BASE_DIR, "screenshots")
os.makedirs(SCREENSHOTS_DIR, exist_ok=True)

# Load data
raw_df = pd.read_csv(RAW_PATH, dtype=str)
clean_df = pd.read_csv(CLEAN_PATH)
with open(REPORT_PATH) as f:
    report = json.load(f)

# --- Color palette ---
DARK_BG = "#0d1117"
CARD_BG = "#161b22"
ACCENT_RED = "#f85149"
ACCENT_GREEN = "#3fb950"
ACCENT_BLUE = "#58a6ff"
ACCENT_YELLOW = "#d29922"
TEXT_LIGHT = "#e6edf3"
TEXT_MUTED = "#8b949e"
GRID_COLOR = "#21262d"


# =============================================================================
# Chart 1: Pipeline Flow Summary
# =============================================================================
def create_pipeline_summary():
    fig, ax = plt.subplots(figsize=(12, 5))
    fig.patch.set_facecolor(DARK_BG)
    ax.set_facecolor(DARK_BG)

    stages = [
        ("Raw Data\n558 rows", ACCENT_RED),
        ("Remove\nInvalid\n-2", ACCENT_YELLOW),
        ("Deduplicate\n-56", ACCENT_YELLOW),
        ("Standardize\nDates", ACCENT_BLUE),
        ("Standardize\nProducts", ACCENT_BLUE),
        ("Clean\nNumerics", ACCENT_BLUE),
        ("Validate\nBiz Rules", ACCENT_GREEN),
        ("Clean Data\n499 rows", ACCENT_GREEN),
    ]

    for i, (label, color) in enumerate(stages):
        x = i * 1.4
        circle = plt.Circle((x, 0.5), 0.42, color=color, alpha=0.15,
                             linewidth=2, edgecolor=color)
        ax.add_patch(circle)
        ax.text(x, 0.5, label, ha="center", va="center",
                fontsize=8.5, color=TEXT_LIGHT, fontweight="bold",
                fontfamily="monospace")
        if i < len(stages) - 1:
            ax.annotate("", xy=((i + 1) * 1.4 - 0.45, 0.5),
                        xytext=(x + 0.45, 0.5),
                        arrowprops=dict(arrowstyle="->", color=TEXT_MUTED,
                                        lw=1.5))

    ax.set_xlim(-0.7, (len(stages) - 1) * 1.4 + 0.7)
    ax.set_ylim(-0.2, 1.2)
    ax.axis("off")
    ax.set_title("ETL Pipeline Flow: 558 → 499 rows (89.4% retention)",
                 color=TEXT_LIGHT, fontsize=14, fontweight="bold", pad=15,
                 fontfamily="monospace")

    plt.tight_layout()
    plt.savefig(os.path.join(SCREENSHOTS_DIR, "pipeline_flow.png"),
                dpi=150, bbox_inches="tight", facecolor=DARK_BG)
    plt.close()
    print("  Saved: pipeline_flow.png")


# =============================================================================
# Chart 2: Data Quality Before vs After
# =============================================================================
def create_quality_comparison():
    fig, axes = plt.subplots(1, 3, figsize=(14, 5))
    fig.patch.set_facecolor(DARK_BG)
    fig.suptitle("Data Quality: Before vs After",
                 color=TEXT_LIGHT, fontsize=16, fontweight="bold",
                 fontfamily="monospace", y=1.02)

    # --- Panel 1: Null percentage by column ---
    ax = axes[0]
    ax.set_facecolor(CARD_BG)
    cols = ["category", "region", "sales_rep", "payment_method", "discount_pct"]
    raw_nulls = []
    clean_nulls = []
    for col in cols:
        raw_null = (raw_df[col].isna().sum() + (raw_df[col] == "").sum()) / len(raw_df) * 100
        raw_nulls.append(raw_null)
        if col in clean_df.columns:
            clean_null = clean_df[col].isna().sum() / len(clean_df) * 100
        else:
            clean_null = 0
        clean_nulls.append(clean_null)

    y = np.arange(len(cols))
    ax.barh(y + 0.15, raw_nulls, 0.3, color=ACCENT_RED, alpha=0.8, label="Before")
    ax.barh(y - 0.15, clean_nulls, 0.3, color=ACCENT_GREEN, alpha=0.8, label="After")
    ax.set_yticks(y)
    ax.set_yticklabels(cols, color=TEXT_LIGHT, fontsize=9, fontfamily="monospace")
    ax.set_xlabel("% Missing", color=TEXT_MUTED, fontsize=10)
    ax.set_title("Null Values", color=TEXT_LIGHT, fontsize=12,
                 fontweight="bold", fontfamily="monospace")
    ax.legend(fontsize=8, loc="lower right", facecolor=CARD_BG,
              edgecolor=GRID_COLOR, labelcolor=TEXT_LIGHT)
    ax.tick_params(colors=TEXT_MUTED)
    ax.spines[:].set_color(GRID_COLOR)

    # --- Panel 2: Issues resolved ---
    ax = axes[1]
    ax.set_facecolor(CARD_BG)
    issues = ["Duplicates", "Date\nFormats", "Product\nNames", "Price\nFormats"]
    counts = [
        report["duplicates_removed"],
        report["dates_standardized"],
        report["products_standardized"],
        report["prices_cleaned"],
    ]
    colors = [ACCENT_RED, ACCENT_BLUE, ACCENT_BLUE, ACCENT_BLUE]
    bars = ax.bar(issues, counts, color=colors, alpha=0.8, width=0.6,
                  edgecolor=[c for c in colors], linewidth=1)
    for bar, count in zip(bars, counts):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 10,
                str(count), ha="center", color=TEXT_LIGHT, fontsize=10,
                fontweight="bold", fontfamily="monospace")
    ax.set_title("Issues Resolved", color=TEXT_LIGHT, fontsize=12,
                 fontweight="bold", fontfamily="monospace")
    ax.tick_params(colors=TEXT_MUTED)
    ax.set_xticklabels(issues, color=TEXT_LIGHT, fontsize=9, fontfamily="monospace")
    ax.spines[:].set_color(GRID_COLOR)

    # --- Panel 3: Data retention donut ---
    ax = axes[2]
    ax.set_facecolor(CARD_BG)
    retained = report["final_row_count"]
    removed = report["initial_row_count"] - retained
    sizes = [retained, removed]
    colors_pie = [ACCENT_GREEN, ACCENT_RED]
    wedges, _ = ax.pie(sizes, colors=colors_pie, startangle=90,
                       wedgeprops=dict(width=0.35, edgecolor=CARD_BG, linewidth=2))
    ax.text(0, 0, f"{retained}\nrows\nretained",
            ha="center", va="center", fontsize=13, fontweight="bold",
            color=TEXT_LIGHT, fontfamily="monospace")
    ax.set_title("Data Retention: 89.4%", color=TEXT_LIGHT, fontsize=12,
                 fontweight="bold", fontfamily="monospace")
    legend = ax.legend(["Retained", "Removed"], loc="lower center",
                       fontsize=8, facecolor=CARD_BG, edgecolor=GRID_COLOR,
                       labelcolor=TEXT_LIGHT)

    plt.tight_layout()
    plt.savefig(os.path.join(SCREENSHOTS_DIR, "quality_comparison.png"),
                dpi=150, bbox_inches="tight", facecolor=DARK_BG)
    plt.close()
    print("  Saved: quality_comparison.png")


# =============================================================================
# Chart 3: Clean Data Overview — Sales by Category & Region
# =============================================================================
def create_clean_data_overview():
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    fig.patch.set_facecolor(DARK_BG)
    fig.suptitle("Clean Data Overview: Ready for Power BI",
                 color=TEXT_LIGHT, fontsize=16, fontweight="bold",
                 fontfamily="monospace", y=1.02)

    # --- Sales by Category ---
    ax = axes[0]
    ax.set_facecolor(CARD_BG)
    cat_sales = clean_df.groupby("category")["total_amount"].sum().sort_values(ascending=True)
    colors = [ACCENT_BLUE, ACCENT_GREEN, ACCENT_YELLOW]
    bars = ax.barh(cat_sales.index, cat_sales.values, color=colors, alpha=0.8,
                   edgecolor=colors, linewidth=1)
    for bar, val in zip(bars, cat_sales.values):
        ax.text(bar.get_width() + 5000, bar.get_y() + bar.get_height() / 2,
                f"${val:,.0f}", va="center", color=TEXT_LIGHT, fontsize=10,
                fontfamily="monospace")
    ax.set_title("Total Sales by Category", color=TEXT_LIGHT, fontsize=12,
                 fontweight="bold", fontfamily="monospace")
    ax.tick_params(colors=TEXT_MUTED)
    ax.set_yticklabels(cat_sales.index, color=TEXT_LIGHT, fontsize=10,
                       fontfamily="monospace")
    ax.spines[:].set_color(GRID_COLOR)
    ax.set_xlabel("Total Amount ($)", color=TEXT_MUTED, fontsize=10)

    # --- Orders by Region ---
    ax = axes[1]
    ax.set_facecolor(CARD_BG)
    region_counts = clean_df["region"].value_counts().sort_values()
    colors_r = [ACCENT_GREEN] * len(region_counts)
    bars = ax.barh(region_counts.index, region_counts.values, color=colors_r,
                   alpha=0.8, edgecolor=ACCENT_GREEN, linewidth=1)
    for bar, val in zip(bars, region_counts.values):
        ax.text(bar.get_width() + 1, bar.get_y() + bar.get_height() / 2,
                str(val), va="center", color=TEXT_LIGHT, fontsize=10,
                fontweight="bold", fontfamily="monospace")
    ax.set_title("Orders by Region", color=TEXT_LIGHT, fontsize=12,
                 fontweight="bold", fontfamily="monospace")
    ax.tick_params(colors=TEXT_MUTED)
    ax.set_yticklabels(region_counts.index, color=TEXT_LIGHT, fontsize=10,
                       fontfamily="monospace")
    ax.spines[:].set_color(GRID_COLOR)
    ax.set_xlabel("Number of Orders", color=TEXT_MUTED, fontsize=10)

    plt.tight_layout()
    plt.savefig(os.path.join(SCREENSHOTS_DIR, "clean_data_overview.png"),
                dpi=150, bbox_inches="tight", facecolor=DARK_BG)
    plt.close()
    print("  Saved: clean_data_overview.png")


if __name__ == "__main__":
    print("Generating portfolio visualizations...")
    create_pipeline_summary()
    create_quality_comparison()
    create_clean_data_overview()
    print("\nAll visualizations saved to screenshots/")

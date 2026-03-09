"""
ETL Data Cleaning Pipeline
===========================
A production-ready pipeline that transforms messy, real-world business data
into clean, analysis-ready datasets for Power BI and other BI tools.

Author: [Your Name] — Data Transformation Manager
Pipeline stages:
    1. Load & Initial Assessment
    2. Remove invalid / test rows
    3. Deduplicate (exact + near-duplicates)
    4. Standardize dates
    5. Standardize product names (fuzzy matching)
    6. Standardize categorical fields (region, category, reps)
    7. Clean numeric fields (prices, quantities)
    8. Validate business rules
    9. Generate data quality report
    10. Export clean dataset
"""

import pandas as pd
import numpy as np
import re
import os
import json
from datetime import datetime
from collections import Counter


# =============================================================================
# CONFIGURATION
# =============================================================================

# Canonical product names for fuzzy matching
CANONICAL_PRODUCTS = {
    "laptop pro 15": "Laptop Pro 15",
    "laptoppro15": "Laptop Pro 15",
    "laptpo pro 15": "Laptop Pro 15",
    "wireless mouse m200": "Wireless Mouse M200",
    "w. mouse m200": "Wireless Mouse M200",
    "wireless mouse m-200": "Wireless Mouse M200",
    "wireles mouse m200": "Wireless Mouse M200",
    "usb-c hub 7-port": "USB-C Hub 7-Port",
    "usb c hub 7 port": "USB-C Hub 7-Port",
    "usbc hub 7port": "USB-C Hub 7-Port",
    "ergonomic chair x1": "Ergonomic Chair X1",
    "ergo chair x1": "Ergonomic Chair X1",
    "ergonomic chair x-1": "Ergonomic Chair X1",
    "standing desk l-shape": "Standing Desk L-Shape",
    "standing desk l shape": "Standing Desk L-Shape",
    "stand desk l-shape": "Standing Desk L-Shape",
    "monitor arm dual": "Monitor Arm Dual",
    "dual monitor arm": "Monitor Arm Dual",
    "monitor arm - dual": "Monitor Arm Dual",
    "notebook a5 premium": "Notebook A5 Premium",
    "notebook a5 prem.": "Notebook A5 Premium",
    "a5 premium notebook": "Notebook A5 Premium",
    "whiteboard markers 12pk": "Whiteboard Markers 12pk",
    "whiteboard markers 12 pk": "Whiteboard Markers 12pk",
    "whiteboard markers (12pk)": "Whiteboard Markers 12pk",
    "wb markers 12pk": "Whiteboard Markers 12pk",
    "printer paper 5000ct": "Printer Paper 5000ct",
    "printer paper 5000 ct": "Printer Paper 5000ct",
    "printer paper (5000ct)": "Printer Paper 5000ct",
    "print paper 5000ct": "Printer Paper 5000ct",
    "desk lamp led": "Desk Lamp LED",
    "led desk lamp": "Desk Lamp LED",
    "desk lamp - led": "Desk Lamp LED",
    "desklamp led": "Desk Lamp LED",
    "webcam hd 1080p": "Webcam HD 1080p",
    "webcam 1080p hd": "Webcam HD 1080p",
    "keyboard mechanical": "Keyboard Mechanical",
    "mechanical keyboard": "Keyboard Mechanical",
    "mech keyboard": "Keyboard Mechanical",
}

CANONICAL_REGIONS = {
    "north": "North", "n": "North", "norte": "North",
    "south": "South", "s": "South", "sur": "South",
    "east": "East", "e": "East", "este": "East",
    "west": "West", "w": "West", "oeste": "West",
    "central": "Central", "c": "Central", "centro": "Central", "cntrl": "Central",
}

CANONICAL_CATEGORIES = {
    "electronics": "Electronics", "electronic": "Electronics",
    "electronicss": "Electronics", "elec.": "Electronics",
    "furniture": "Furniture", "furnitures": "Furniture",
    "furn.": "Furniture", "furntiure": "Furniture",
    "office supplies": "Office Supplies", "office supply": "Office Supplies",
    "off. supplies": "Office Supplies",
}

# Known sales reps for name resolution
KNOWN_REPS = [
    "Maria Rodriguez", "James Chen", "Sarah Williams",
    "Carlos Mendez", "Aisha Patel", "David Kim",
    "Elena Vasquez", "Robert Johnson", "Fatima Al-Hassan",
    "Thomas Brown",
]


# =============================================================================
# PIPELINE STAGES
# =============================================================================

class DataQualityReport:
    """Tracks data quality metrics across pipeline stages."""

    def __init__(self):
        self.metrics = {
            "initial_row_count": 0,
            "final_row_count": 0,
            "duplicates_removed": 0,
            "invalid_rows_removed": 0,
            "dates_standardized": 0,
            "dates_unparseable": 0,
            "products_standardized": 0,
            "products_unresolved": 0,
            "prices_cleaned": 0,
            "nulls_found": {},
            "nulls_filled": {},
            "business_rule_violations": [],
            "stage_log": [],
        }

    def log(self, stage, message, count=None):
        entry = {"stage": stage, "message": message, "count": count}
        self.metrics["stage_log"].append(entry)
        count_str = f" ({count})" if count is not None else ""
        print(f"  [{stage}] {message}{count_str}")

    def summary(self):
        return self.metrics


def stage_1_load(filepath):
    """Load raw data and perform initial assessment."""
    df = pd.read_csv(filepath, dtype=str)  # Load everything as string initially
    report = DataQualityReport()
    report.metrics["initial_row_count"] = len(df)
    report.log("LOAD", f"Loaded raw data", count=len(df))
    report.log("LOAD", f"Columns: {list(df.columns)}")

    # Assess nulls per column
    for col in df.columns:
        null_count = df[col].isna().sum() + (df[col] == "").sum()
        if null_count > 0:
            report.metrics["nulls_found"][col] = int(null_count)

    report.log("LOAD", f"Null/empty values found: {report.metrics['nulls_found']}")
    return df, report


def stage_2_remove_invalid(df, report):
    """Remove test rows, completely empty rows, and clearly invalid entries."""
    initial = len(df)

    # Remove rows where order_id is empty or missing
    df = df[df["order_id"].notna() & (df["order_id"].str.strip() != "")]

    # Remove rows with test/placeholder notes
    test_patterns = r"(?i)test row|test data|ignore|placeholder"
    mask = df["notes"].fillna("").str.contains(test_patterns, regex=True)
    df = df[~mask]

    # Remove rows with clearly invalid regions
    invalid_regions = ["narnia", "???", "unknown", "test"]
    mask = df["region"].fillna("").str.strip().str.lower().isin(invalid_regions)
    df = df[~mask]

    # Remove rows with non-numeric quantities
    def is_valid_quantity(x):
        try:
            val = int(float(x))
            return val > 0
        except (ValueError, TypeError):
            return False

    df = df[df["quantity"].apply(is_valid_quantity)]

    removed = initial - len(df)
    report.metrics["invalid_rows_removed"] = removed
    report.log("INVALID", f"Removed invalid/test rows", count=removed)
    return df


def stage_3_deduplicate(df, report):
    """Remove exact duplicates and near-duplicates."""
    initial = len(df)

    # Exact duplicates
    df = df.drop_duplicates()
    after_exact = len(df)
    exact_dupes = initial - after_exact

    # Near-duplicates: same order_id, keep first occurrence
    df = df.drop_duplicates(subset=["order_id"], keep="first")
    after_near = len(df)
    near_dupes = after_exact - after_near

    total_removed = initial - len(df)
    report.metrics["duplicates_removed"] = total_removed
    report.log("DEDUP", f"Exact duplicates removed", count=exact_dupes)
    report.log("DEDUP", f"Near-duplicates removed (by order_id)", count=near_dupes)
    return df


def stage_4_standardize_dates(df, report):
    """Parse dates from multiple formats into a consistent YYYY-MM-DD format."""
    date_formats = [
        "%m/%d/%Y", "%Y-%m-%d", "%d-%b-%y", "%B %d, %Y",
        "%m-%d-%Y", "%d/%m/%Y", "%Y/%m/%d",
    ]
    parsed_count = 0
    unparsed_count = 0
    cutoff_date = datetime(2024, 12, 31)

    def parse_date(date_str):
        nonlocal parsed_count, unparsed_count
        if pd.isna(date_str) or str(date_str).strip() == "":
            unparsed_count += 1
            return pd.NaT

        date_str = str(date_str).strip()
        for fmt in date_formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                # Validate: not in future beyond our data range
                if dt > cutoff_date:
                    unparsed_count += 1
                    return pd.NaT
                parsed_count += 1
                return dt
            except ValueError:
                continue

        unparsed_count += 1
        return pd.NaT

    df["order_date"] = df["order_date"].apply(parse_date)

    # Drop rows where date could not be parsed
    before_drop = len(df)
    df = df.dropna(subset=["order_date"])
    dropped = before_drop - len(df)

    report.metrics["dates_standardized"] = parsed_count
    report.metrics["dates_unparseable"] = unparsed_count
    report.log("DATES", f"Dates parsed successfully", count=parsed_count)
    report.log("DATES", f"Unparseable/future dates → rows dropped", count=dropped)
    return df


def stage_5_standardize_products(df, report):
    """Map messy product names to canonical versions using lookup table."""
    resolved = 0
    unresolved = 0
    unresolved_names = []

    def map_product(name):
        nonlocal resolved, unresolved
        if pd.isna(name) or str(name).strip() == "":
            unresolved += 1
            return None

        cleaned = re.sub(r"\s+", " ", str(name).strip().lower())
        if cleaned in CANONICAL_PRODUCTS:
            resolved += 1
            return CANONICAL_PRODUCTS[cleaned]
        else:
            unresolved += 1
            unresolved_names.append(name)
            return None

    df["product_name"] = df["product_name"].apply(map_product)

    # Drop unresolved products
    before = len(df)
    df = df.dropna(subset=["product_name"])
    dropped = before - len(df)

    report.metrics["products_standardized"] = resolved
    report.metrics["products_unresolved"] = unresolved
    if unresolved_names:
        report.log("PRODUCTS", f"Unresolved product names: {set(unresolved_names)}")
    report.log("PRODUCTS", f"Products standardized", count=resolved)
    report.log("PRODUCTS", f"Unresolved → rows dropped", count=dropped)
    return df


def stage_6_standardize_categoricals(df, report):
    """Standardize region, category, sales rep, and other text fields."""

    # --- Region ---
    def map_region(val):
        if pd.isna(val):
            return np.nan
        cleaned = str(val).strip().lower()
        return CANONICAL_REGIONS.get(cleaned, np.nan)

    df["region"] = df["region"].apply(map_region)
    report.log("CATEGORICALS", f"Regions standardized, remaining nulls: {df['region'].isna().sum()}")

    # --- Category ---
    def map_category(val):
        if pd.isna(val):
            return np.nan
        cleaned = re.sub(r"\s+", " ", str(val).strip().lower())
        return CANONICAL_CATEGORIES.get(cleaned, np.nan)

    df["category"] = df["category"].apply(map_category)

    # Fill missing categories from product name lookup
    product_category = {
        "Laptop Pro 15": "Electronics", "Wireless Mouse M200": "Electronics",
        "USB-C Hub 7-Port": "Electronics", "Webcam HD 1080p": "Electronics",
        "Keyboard Mechanical": "Electronics",
        "Ergonomic Chair X1": "Furniture", "Standing Desk L-Shape": "Furniture",
        "Monitor Arm Dual": "Furniture",
        "Notebook A5 Premium": "Office Supplies",
        "Whiteboard Markers 12pk": "Office Supplies",
        "Printer Paper 5000ct": "Office Supplies", "Desk Lamp LED": "Office Supplies",
    }
    mask = df["category"].isna()
    df.loc[mask, "category"] = df.loc[mask, "product_name"].map(product_category)
    filled = mask.sum() - df["category"].isna().sum()
    report.log("CATEGORICALS", f"Categories inferred from product names", count=filled)

    # --- Sales Rep ---
    rep_lookup = {}
    for name in KNOWN_REPS:
        parts = name.split()
        first, last = parts[0], parts[-1]
        # Multiple lookup keys for each rep
        rep_lookup[name.lower()] = name
        rep_lookup[f"{last}, {first}".lower()] = name
        rep_lookup[f"{first[0]}. {last}".lower()] = name

    def map_rep(val):
        if pd.isna(val):
            return np.nan
        cleaned = re.sub(r"\s+", " ", str(val).strip().lower())
        if cleaned in rep_lookup:
            return rep_lookup[cleaned]
        # Try partial matching
        for key, canonical in rep_lookup.items():
            if cleaned in key or key in cleaned:
                return canonical
        return np.nan

    df["sales_rep"] = df["sales_rep"].apply(map_rep)
    report.log("CATEGORICALS", f"Sales reps standardized, remaining nulls: {df['sales_rep'].isna().sum()}")

    # --- Customer Type (simple strip + title case) ---
    df["customer_type"] = df["customer_type"].str.strip().str.title()

    # --- Payment Method (simple strip + title case) ---
    df["payment_method"] = df["payment_method"].str.strip().str.title()

    return df


def stage_7_clean_numerics(df, report):
    """Clean and standardize numeric fields: prices, quantities, discounts."""
    cleaned_count = 0

    def clean_price(val):
        nonlocal cleaned_count
        if pd.isna(val):
            return np.nan
        val_str = str(val).strip()
        # Remove dollar signs, commas, spaces
        val_str = re.sub(r"[$,\s]", "", val_str)
        try:
            result = float(val_str)
            cleaned_count += 1
            return abs(result)  # Fix accidental negatives
        except ValueError:
            return np.nan

    df["unit_price"] = df["unit_price"].apply(clean_price)
    df["total_amount"] = df["total_amount"].apply(clean_price)
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").astype("Int64")

    # Recalculate total where possible (unit_price × quantity)
    mask = df["unit_price"].notna() & df["quantity"].notna()
    df.loc[mask, "total_amount_calc"] = (
        df.loc[mask, "unit_price"] * df.loc[mask, "quantity"].astype(float)
    ).round(2)

    # Use calculated total if original is missing or differs significantly
    diff_mask = mask & (
        df["total_amount"].isna()
        | (abs(df["total_amount"] - df["total_amount_calc"]) > 0.02)
    )
    df.loc[diff_mask, "total_amount"] = df.loc[diff_mask, "total_amount_calc"]
    df = df.drop(columns=["total_amount_calc"], errors="ignore")

    # Clean discount
    df["discount_pct"] = pd.to_numeric(df["discount_pct"], errors="coerce").fillna(0)
    df.loc[df["discount_pct"] > 100, "discount_pct"] = 0  # Invalid discounts
    df.loc[df["discount_pct"] < 0, "discount_pct"] = 0

    report.metrics["prices_cleaned"] = cleaned_count
    report.log("NUMERICS", f"Price values cleaned", count=cleaned_count)

    # Clean notes — standardize null-like values
    null_notes = ["n/a", "na", "null", "-", "none", ""]
    df["notes"] = df["notes"].fillna("").str.strip()
    df.loc[df["notes"].str.lower().isin(null_notes), "notes"] = ""

    return df


def stage_8_validate(df, report):
    """Apply business rule validations."""
    violations = []

    # Rule 1: unit_price should be positive
    mask = df["unit_price"] <= 0
    if mask.any():
        violations.append(f"Non-positive unit_price: {mask.sum()} rows")
        df = df[~mask]

    # Rule 2: quantity should be between 1 and 10000
    mask = (df["quantity"] < 1) | (df["quantity"] > 10000)
    if mask.any():
        violations.append(f"Quantity out of range [1, 10000]: {mask.sum()} rows")
        df = df[~mask]

    # Rule 3: order_date should be within expected range
    min_date = pd.Timestamp("2023-01-01")
    max_date = pd.Timestamp("2024-12-31")
    mask = (df["order_date"] < min_date) | (df["order_date"] > max_date)
    if mask.any():
        violations.append(f"Dates outside 2023-2024 range: {mask.sum()} rows")
        df = df[~mask]

    report.metrics["business_rule_violations"] = violations
    for v in violations:
        report.log("VALIDATE", v)

    return df


def stage_9_finalize(df, report):
    """Final formatting and column ordering for BI-readiness."""
    # Format date as string for CSV export
    df["order_date"] = pd.to_datetime(df["order_date"]).dt.strftime("%Y-%m-%d")

    # Round numeric columns
    df["unit_price"] = df["unit_price"].round(2)
    df["total_amount"] = df["total_amount"].round(2)
    df["discount_pct"] = df["discount_pct"].astype(int)

    # Reorder columns
    column_order = [
        "order_id", "order_date", "product_name", "category",
        "quantity", "unit_price", "total_amount",
        "region", "sales_rep", "customer_type",
        "payment_method", "discount_pct", "notes"
    ]
    df = df[column_order]

    # Sort
    df = df.sort_values(["order_date", "order_id"]).reset_index(drop=True)

    report.metrics["final_row_count"] = len(df)
    report.log("FINALIZE", f"Final clean dataset", count=len(df))
    return df


# =============================================================================
# MAIN PIPELINE
# =============================================================================

def run_pipeline(input_path, output_path, report_path=None):
    """Execute the full ETL pipeline."""
    print("=" * 60)
    print("  ETL DATA CLEANING PIPELINE")
    print("=" * 60)
    print()

    # Stage 1: Load
    print("Stage 1: Loading data...")
    df, report = stage_1_load(input_path)
    print()

    # Stage 2: Remove invalid
    print("Stage 2: Removing invalid rows...")
    df = stage_2_remove_invalid(df, report)
    print()

    # Stage 3: Deduplicate
    print("Stage 3: Deduplicating...")
    df = stage_3_deduplicate(df, report)
    print()

    # Stage 4: Standardize dates
    print("Stage 4: Standardizing dates...")
    df = stage_4_standardize_dates(df, report)
    print()

    # Stage 5: Standardize products
    print("Stage 5: Standardizing product names...")
    df = stage_5_standardize_products(df, report)
    print()

    # Stage 6: Standardize categoricals
    print("Stage 6: Standardizing categorical fields...")
    df = stage_6_standardize_categoricals(df, report)
    print()

    # Stage 7: Clean numerics
    print("Stage 7: Cleaning numeric fields...")
    df = stage_7_clean_numerics(df, report)
    print()

    # Stage 8: Validate
    print("Stage 8: Applying business rules...")
    df = stage_8_validate(df, report)
    print()

    # Stage 9: Finalize
    print("Stage 9: Finalizing dataset...")
    df = stage_9_finalize(df, report)
    print()

    # Export
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"Clean data saved to: {output_path}")

    # Quality report
    if report_path:
        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        summary = report.summary()
        with open(report_path, "w") as f:
            json.dump(summary, f, indent=2, default=str)
        print(f"Quality report saved to: {report_path}")

    # Print summary
    print()
    print("=" * 60)
    print("  PIPELINE SUMMARY")
    print("=" * 60)
    s = report.summary()
    print(f"  Rows in:           {s['initial_row_count']}")
    print(f"  Invalid removed:   {s['invalid_rows_removed']}")
    print(f"  Duplicates removed:{s['duplicates_removed']}")
    print(f"  Dates parsed:      {s['dates_standardized']}")
    print(f"  Products matched:  {s['products_standardized']}")
    print(f"  Prices cleaned:    {s['prices_cleaned']}")
    print(f"  Rows out:          {s['final_row_count']}")
    retention = (s['final_row_count'] / s['initial_row_count'] * 100)
    print(f"  Data retention:    {retention:.1f}%")
    print("=" * 60)

    return df, report


if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.dirname(__file__))
    input_path = os.path.join(base_dir, "data", "raw", "messy_sales_data.csv")
    output_path = os.path.join(base_dir, "data", "cleaned", "clean_sales_data.csv")
    report_path = os.path.join(base_dir, "data", "cleaned", "quality_report.json")

    run_pipeline(input_path, output_path, report_path)

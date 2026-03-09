"""
Unit tests for the ETL data cleaning pipeline.
Validates each transformation stage independently.
"""

import pandas as pd
import numpy as np
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from clean import (
    stage_1_load, stage_2_remove_invalid, stage_3_deduplicate,
    stage_7_clean_numerics, DataQualityReport
)


def test_price_cleaning():
    """Prices in various formats should all resolve to clean floats."""
    report = DataQualityReport()
    df = pd.DataFrame({
        "order_id": ["A", "B", "C", "D", "E"],
        "unit_price": ["$1,299.99", "49.99", "$29.99", "free", "-15.00"],
        "total_amount": ["$2,599.98", "99.98", "$29.99", "", "15.00"],
        "quantity": ["2", "2", "1", "1", "1"],
        "discount_pct": ["10", "", "abc", "150", "-5"],
        "notes": ["ok", "N/A", "null", "-", "none"],
    })
    result = stage_7_clean_numerics(df, report)

    assert result["unit_price"].iloc[0] == 1299.99, "Dollar+comma format failed"
    assert result["unit_price"].iloc[1] == 49.99, "Plain numeric failed"
    assert result["unit_price"].iloc[2] == 29.99, "Dollar format failed"
    assert pd.isna(result["unit_price"].iloc[3]), "Non-numeric should be NaN"
    assert result["unit_price"].iloc[4] == 15.00, "Negative should become positive"

    # Discount validation
    assert result["discount_pct"].iloc[0] == 10
    assert result["discount_pct"].iloc[1] == 0    # Empty → 0
    assert result["discount_pct"].iloc[2] == 0    # Non-numeric → 0
    assert result["discount_pct"].iloc[3] == 0    # >100 → 0
    assert result["discount_pct"].iloc[4] == 0    # Negative → 0

    # Notes cleanup
    assert result["notes"].iloc[1] == ""  # N/A → empty
    assert result["notes"].iloc[2] == ""  # null → empty
    assert result["notes"].iloc[3] == ""  # - → empty

    print("  PASS: test_price_cleaning")


def test_deduplication():
    """Exact and near-duplicates should be removed correctly."""
    report = DataQualityReport()
    df = pd.DataFrame({
        "order_id": ["ORD-001", "ORD-001", "ORD-002", "ORD-002", "ORD-003"],
        "order_date": ["2024-01-01"] * 5,
        "product_name": ["Widget"] * 5,
        "category": ["Cat"] * 5,
        "quantity": ["10", "10", "5", "6", "3"],  # ORD-002 is near-dupe
        "unit_price": ["10"] * 5,
        "total_amount": ["100", "100", "50", "60", "30"],
        "region": ["North"] * 5,
        "sales_rep": ["Rep"] * 5,
        "customer_type": ["Retail"] * 5,
        "payment_method": ["Card"] * 5,
        "discount_pct": ["0"] * 5,
        "notes": [""] * 5,
    })
    result = stage_3_deduplicate(df, report)
    assert len(result) == 3, f"Expected 3 rows, got {len(result)}"
    assert set(result["order_id"]) == {"ORD-001", "ORD-002", "ORD-003"}
    print("  PASS: test_deduplication")


def test_invalid_removal():
    """Test rows, empty IDs, bad quantities should be removed."""
    report = DataQualityReport()
    df = pd.DataFrame({
        "order_id": ["ORD-001", "", "ORD-003", "ORD-004"],
        "order_date": ["2024-01-01"] * 4,
        "product_name": ["Widget"] * 4,
        "category": ["Cat"] * 4,
        "quantity": ["5", "10", "abc", "3"],
        "unit_price": ["10"] * 4,
        "total_amount": ["50"] * 4,
        "region": ["North", "North", "North", "Narnia"],
        "sales_rep": ["Rep"] * 4,
        "customer_type": ["Retail"] * 4,
        "payment_method": ["Card"] * 4,
        "discount_pct": ["0"] * 4,
        "notes": ["", "TEST ROW - IGNORE", "", "Bad data entry"],
    })
    result = stage_2_remove_invalid(df, report)
    assert len(result) == 1, f"Expected 1 valid row, got {len(result)}"
    assert result.iloc[0]["order_id"] == "ORD-001"
    print("  PASS: test_invalid_removal")


def test_full_pipeline_output():
    """Validate the full pipeline output meets BI-readiness criteria."""
    clean_path = os.path.join(
        os.path.dirname(__file__), "..", "data", "cleaned", "clean_sales_data.csv"
    )
    if not os.path.exists(clean_path):
        print("  SKIP: test_full_pipeline_output (clean data not found)")
        return

    df = pd.read_csv(clean_path)

    # No duplicate order_ids
    assert df["order_id"].is_unique, "Duplicate order_ids found"

    # All dates parseable
    dates = pd.to_datetime(df["order_date"], format="%Y-%m-%d", errors="coerce")
    assert dates.notna().all(), "Unparseable dates in output"

    # Date range
    assert dates.min() >= pd.Timestamp("2023-01-01"), "Dates before 2023"
    assert dates.max() <= pd.Timestamp("2024-12-31"), "Dates after 2024"

    # All prices are numeric and positive
    assert (df["unit_price"] > 0).all(), "Non-positive unit prices"
    assert (df["total_amount"] > 0).all(), "Non-positive totals"
    assert (df["quantity"] > 0).all(), "Non-positive quantities"

    # Discount in valid range
    assert (df["discount_pct"] >= 0).all() and (df["discount_pct"] <= 100).all()

    # Categories are from known set
    valid_cats = {"Electronics", "Furniture", "Office Supplies"}
    assert set(df["category"].dropna().unique()).issubset(valid_cats)

    # Regions are from known set
    valid_regions = {"North", "South", "East", "West", "Central"}
    assert set(df["region"].dropna().unique()).issubset(valid_regions)

    print(f"  PASS: test_full_pipeline_output ({len(df)} rows validated)")


if __name__ == "__main__":
    print("\nRunning ETL Pipeline Tests...")
    print("-" * 40)
    test_price_cleaning()
    test_deduplication()
    test_invalid_removal()
    test_full_pipeline_output()
    print("-" * 40)
    print("All tests passed!")

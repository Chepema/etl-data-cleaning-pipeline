# ETL Data Cleaning Pipeline

**A production-ready Python pipeline that transforms messy, real-world business data into clean, analysis-ready datasets for Power BI and other BI tools.**

This project demonstrates the data preparation work that precedes every dashboard I build. Most dashboard failures happen because this step was skipped.

![Pipeline Flow](https://github.com/Chepema/etl-data-cleaning-pipeline/blob/main/pipeline_flow.png)
---

## The Problem

Small and mid-size businesses accumulate data in spreadsheets that look like this:

| order_id | order_date | product_name | unit_price | region |
|----------|-----------|--------------|------------|--------|
| ORD-10158 | 2023/07/03 | Printer Paper 5000 ct | $36.65 | West |
| ORD-10451 | 25/05/2024 | keyboard mechanical | 164.85 | S |
| ORD-10055 | 01-06-2024 | Notebook A5  Premium | $11.77 | |
| ORD-10247 | 2024-12-19 | wireless mouse m200 | $999.72 | West  |
| ORD-10070 | 05/19/2024 | laptop pro 15 | 1223.69 | East |
| ORD-10376 | January 08, 2024 | usb-c hub 7-port | $46.36 | WEST |

**7 different date formats. Inconsistent product names. Mixed price formats. Missing regions. Duplicates everywhere.**

You can't build a reliable dashboard on this. The pipeline fixes all of it.

---

## The Solution

A 9-stage pipeline that systematically resolves every data quality issue:

1. **Load & Assess** — Profile the raw data, identify issues
2. **Remove Invalid** — Drop test rows, empty records, clearly bad entries
3. **Deduplicate** — Remove exact duplicates and near-duplicates (same order_id)
4. **Standardize Dates** — Parse 7+ date formats → consistent `YYYY-MM-DD`
5. **Standardize Products** — Fuzzy match 60+ name variations → 12 canonical products
6. **Standardize Categoricals** — Normalize regions, categories, sales rep names
7. **Clean Numerics** — Strip currency symbols, fix negatives, recalculate totals
8. **Validate Business Rules** — Enforce date ranges, positive prices, valid quantities
9. **Finalize** — Format, sort, and export BI-ready dataset

---

## Results

![Data Quality Comparison](https://github.com/Chepema/etl-data-cleaning-pipeline/blob/main/quality_comparison.png)

| Metric | Value |
|--------|-------|
| Rows in | 558 |
| Rows out | 499 |
| Data retention | 89.4% |
| Duplicates removed | 56 |
| Date formats standardized | 499 |
| Product names resolved | 499 |
| Price values cleaned | 998 |

### Clean Data Overview

![Clean Data Overview](https://github.com/Chepema/etl-data-cleaning-pipeline/blob/main/clean_data_overview.png)

---

## Project Structure

```
etl-data-cleaning-pipeline/
├── README.md
├── requirements.txt
├── LICENSE
├── data/
│   ├── raw/
│   │   └── messy_sales_data.csv          # 558 rows of realistic messy data
│   └── cleaned/
│       ├── clean_sales_data.csv          # 499 rows, BI-ready
│       └── quality_report.json           # Full pipeline metrics
├── src/
│   ├── generate_messy_data.py            # Creates the sample messy dataset
│   ├── clean.py                          # Main cleaning pipeline (9 stages)
│   └── visualize.py                      # Generates README visualizations
├── tests/
│   └── test_clean.py                     # Unit tests for each stage
└── screenshots/
    ├── pipeline_flow.png
    ├── quality_comparison.png
    └── clean_data_overview.png
```

---

## Quick Start

```bash
# Clone the repo
git clone https://github.com/[your-username]/etl-data-cleaning-pipeline.git
cd etl-data-cleaning-pipeline

# Install dependencies
pip install -r requirements.txt

# Run the pipeline
python src/clean.py

# Run tests
python tests/test_clean.py

# (Optional) Regenerate the messy data
python src/generate_messy_data.py

# (Optional) Regenerate visualizations
python src/visualize.py
```

---

## How It Works

### Date Standardization

The pipeline handles 7+ date formats found in real business spreadsheets:

| Input Format | Example | Parsed Output |
|-------------|---------|---------------|
| MM/DD/YYYY | 01/15/2024 | 2024-01-15 |
| YYYY-MM-DD | 2024-01-15 | 2024-01-15 |
| DD-Mon-YY | 15-Jan-24 | 2024-01-15 |
| Month DD, YYYY | January 15, 2024 | 2024-01-15 |
| MM-DD-YYYY | 01-15-2024 | 2024-01-15 |
| DD/MM/YYYY | 15/01/2024 | 2024-01-15 |
| YYYY/MM/DD | 2024/01/15 | 2024-01-15 |

### Product Name Resolution

Maps 60+ messy variations to 12 canonical product names:

```
"laptop pro 15"          → Laptop Pro 15
"LAPTOP PRO 15"          → Laptop Pro 15
"Laptpo Pro 15"          → Laptop Pro 15    (typo)
"LaptopPro15"            → Laptop Pro 15    (no spaces)
" Laptop Pro 15 "        → Laptop Pro 15    (whitespace)
```

### Price Cleaning

Handles mixed currency formats in the same column:

```
"$1,299.99"  → 1299.99     (dollar sign + commas)
"49.99"      → 49.99       (plain numeric)
"$29.99"     → 29.99       (dollar sign)
"-15.00"     → 15.00       (accidental negative → absolute value)
"free"       → NaN         (non-numeric → null)
```

---

## Adapting for Your Data

This pipeline is designed to be reused. To adapt it for a client project:

1. **Replace the canonical lookup tables** in `clean.py` with the client's product names, regions, and categories
2. **Adjust business rules** in `stage_8_validate()` for the client's date ranges and value thresholds
3. **Run `stage_1_load()`** first to profile the new data and identify which stages need customization

The modular stage-based architecture means you can enable, disable, or reorder stages based on each dataset's specific issues.

---

## Tech Stack

- **Python 3.10+** — Core pipeline logic
- **Pandas** — Data manipulation and transformation
- **NumPy** — Numeric operations
- **Matplotlib** — Data quality visualizations

---

## About

Built by **José María Bolaños Corredera** — Data Transformation Manager with 8+ years in project management, IT operations, and international development.

This pipeline reflects the same approach I used to design data systems at UNICEF (tracking $20M+ in contracts) and in enterprise retail analytics at Best Buy/Accenture — scaled down to a reproducible, open-source template.

**Certifications:** Power BI · Python · Scrum Master · Lean Six Sigma · Design Thinking

blns3191@gmail.com · www.linkedin.com/in/jose-maria-bolanos-corredera-80a339a9 · https://www.fiverr.com/s/99vv47Y

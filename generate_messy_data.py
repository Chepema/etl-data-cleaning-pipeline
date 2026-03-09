"""
Generate a realistic messy sales dataset that simulates common data quality
issues found in small-to-mid business environments.

Issues introduced:
- Duplicate records (exact and near-duplicates)
- Inconsistent date formats (MM/DD/YYYY, YYYY-MM-DD, DD-Mon-YY, etc.)
- Mixed case and spelling variations in categorical fields
- Missing values (random and systematic)
- Whitespace and special character contamination
- Inconsistent currency formatting ($1,234.56 vs 1234.56 vs $1234)
- Invalid entries (negative quantities, future dates, typos)
- Mixed data types in single columns
- Encoding artifacts
"""

import pandas as pd
import numpy as np
import random
import os
from datetime import datetime, timedelta

random.seed(42)
np.random.seed(42)

# --- Configuration ---
N_RECORDS = 500
DUPLICATE_RATE = 0.08
MISSING_RATE = 0.06

# --- Reference Data ---
PRODUCTS = {
    "Laptop Pro 15": {"category": "Electronics", "base_price": 1299.99},
    "Wireless Mouse M200": {"category": "Electronics", "base_price": 29.99},
    "USB-C Hub 7-Port": {"category": "Electronics", "base_price": 49.99},
    "Ergonomic Chair X1": {"category": "Furniture", "base_price": 599.00},
    "Standing Desk L-Shape": {"category": "Furniture", "base_price": 849.00},
    "Monitor Arm Dual": {"category": "Furniture", "base_price": 129.99},
    "Notebook A5 Premium": {"category": "Office Supplies", "base_price": 12.99},
    "Whiteboard Markers 12pk": {"category": "Office Supplies", "base_price": 8.49},
    "Printer Paper 5000ct": {"category": "Office Supplies", "base_price": 34.99},
    "Desk Lamp LED": {"category": "Office Supplies", "base_price": 45.00},
    "Webcam HD 1080p": {"category": "Electronics", "base_price": 79.99},
    "Keyboard Mechanical": {"category": "Electronics", "base_price": 149.99},
}

REGIONS = ["North", "South", "East", "West", "Central"]
SALES_REPS = [
    "Maria Rodriguez", "James Chen", "Sarah Williams",
    "Carlos Mendez", "Aisha Patel", "David Kim",
    "Elena Vasquez", "Robert Johnson", "Fatima Al-Hassan",
    "Thomas Brown"
]
CUSTOMER_TYPES = ["Retail", "Wholesale", "Online", "Corporate"]
PAYMENT_METHODS = ["Credit Card", "Wire Transfer", "Check", "PayPal", "Net 30"]

# --- Messification Functions ---

def mess_up_date(date_obj):
    """Return date in random inconsistent format."""
    formats = [
        "%m/%d/%Y",       # 01/15/2024
        "%Y-%m-%d",       # 2024-01-15
        "%d-%b-%y",       # 15-Jan-24
        "%B %d, %Y",      # January 15, 2024
        "%m-%d-%Y",       # 01-15-2024
        "%d/%m/%Y",       # 15/01/2024 (ambiguous!)
        "%Y/%m/%d",       # 2024/01/15
    ]
    fmt = random.choice(formats)
    return date_obj.strftime(fmt)


def mess_up_product_name(name):
    """Introduce typos and inconsistencies in product names."""
    variations = {
        "Laptop Pro 15": [
            "Laptop Pro 15", "laptop pro 15", "LAPTOP PRO 15",
            "Laptop Pro  15", " Laptop Pro 15", "Laptpo Pro 15",
            "Laptop Pro 15 ", "LaptopPro15"
        ],
        "Wireless Mouse M200": [
            "Wireless Mouse M200", "wireless mouse m200",
            "Wireless Mouse  M200", "Wireles Mouse M200",
            "W. Mouse M200", "Wireless Mouse M-200"
        ],
        "USB-C Hub 7-Port": [
            "USB-C Hub 7-Port", "usb-c hub 7-port", "USB C Hub 7 Port",
            "USBC Hub 7Port", "USB-C Hub 7-port", "USB-C  Hub 7-Port"
        ],
        "Ergonomic Chair X1": [
            "Ergonomic Chair X1", "ergonomic chair x1",
            "Ergo Chair X1", "Ergonomic Chair  X1",
            "Ergonomic Chair X-1", "ERGONOMIC CHAIR X1"
        ],
        "Standing Desk L-Shape": [
            "Standing Desk L-Shape", "standing desk l-shape",
            "Standing Desk L Shape", "Stand Desk L-Shape",
            "Standing Desk L-shape", "Standing  Desk L-Shape"
        ],
        "Monitor Arm Dual": [
            "Monitor Arm Dual", "monitor arm dual",
            "Monitor Arm  Dual", "Dual Monitor Arm",
            "Monitor Arm - Dual", "MONITOR ARM DUAL"
        ],
        "Notebook A5 Premium": [
            "Notebook A5 Premium", "notebook a5 premium",
            "NoteBook A5 Premium", "Notebook A5  Premium",
            "Notebook A5 Prem.", "A5 Premium Notebook"
        ],
        "Whiteboard Markers 12pk": [
            "Whiteboard Markers 12pk", "whiteboard markers 12pk",
            "Whiteboard Markers 12 pk", "WB Markers 12pk",
            "Whiteboard Markers (12pk)", "Whiteboard  Markers 12pk"
        ],
        "Printer Paper 5000ct": [
            "Printer Paper 5000ct", "printer paper 5000ct",
            "Printer Paper 5000 ct", "Print Paper 5000ct",
            "Printer Paper (5000ct)", "PRINTER PAPER 5000CT"
        ],
        "Desk Lamp LED": [
            "Desk Lamp LED", "desk lamp led", "Desk Lamp  LED",
            "LED Desk Lamp", "Desk Lamp - LED", "DeskLamp LED"
        ],
        "Webcam HD 1080p": [
            "Webcam HD 1080p", "webcam hd 1080p", "Webcam HD  1080p",
            "Webcam 1080p HD", "WebCam HD 1080P", "WEBCAM HD 1080P"
        ],
        "Keyboard Mechanical": [
            "Keyboard Mechanical", "keyboard mechanical",
            "Mech Keyboard", "Mechanical Keyboard",
            "Keyboard Mechanical ", "KEYBOARD MECHANICAL"
        ],
    }
    return random.choice(variations.get(name, [name]))


def mess_up_category(category):
    """Introduce inconsistencies in category names."""
    variations = {
        "Electronics": [
            "Electronics", "electronics", "ELECTRONICS",
            "Electronicss", "Electronic", "Elec."
        ],
        "Furniture": [
            "Furniture", "furniture", "FURNITURE",
            "Furnitures", "Furn.", "Furntiure"
        ],
        "Office Supplies": [
            "Office Supplies", "office supplies", "OFFICE SUPPLIES",
            "Office supplies", "Office Supply", "Off. Supplies",
            "Office  Supplies"
        ],
    }
    return random.choice(variations.get(category, [category]))


def mess_up_price(price):
    """Return price in inconsistent formats."""
    fmt = random.choice(["clean", "dollar", "dollar_comma", "string", "negative"])
    if fmt == "clean":
        return round(price, 2)
    elif fmt == "dollar":
        return f"${price:.2f}"
    elif fmt == "dollar_comma":
        return f"${price:,.2f}"
    elif fmt == "string":
        return str(round(price, 2))
    elif fmt == "negative":
        # Rare: accidental negative (refund entered wrong)
        return round(-price, 2) if random.random() < 0.02 else round(price, 2)


def mess_up_region(region):
    """Inconsistent region entries."""
    variations = {
        "North": ["North", "north", "N", "NORTH", "Norte", " North"],
        "South": ["South", "south", "S", "SOUTH", "Sur", "South "],
        "East": ["East", "east", "E", "EAST", "Este", " East "],
        "West": ["West", "west", "W", "WEST", "Oeste", "West  "],
        "Central": ["Central", "central", "C", "CENTRAL", "Centro", "Cntrl"],
    }
    return random.choice(variations.get(region, [region]))


def mess_up_rep_name(name):
    """Inconsistent rep name formatting."""
    first, last = name.split(" ", 1)
    fmt = random.choice([
        "normal", "lower", "upper", "last_first", "initial", "extra_space"
    ])
    if fmt == "normal":
        return name
    elif fmt == "lower":
        return name.lower()
    elif fmt == "upper":
        return name.upper()
    elif fmt == "last_first":
        return f"{last}, {first}"
    elif fmt == "initial":
        return f"{first[0]}. {last}"
    elif fmt == "extra_space":
        return f" {first}  {last} "


# --- Generate Base Data ---
records = []
start_date = datetime(2023, 1, 1)
end_date = datetime(2024, 12, 31)
date_range_days = (end_date - start_date).days

for i in range(N_RECORDS):
    product_name = random.choice(list(PRODUCTS.keys()))
    product_info = PRODUCTS[product_name]
    quantity = random.randint(1, 50)
    base_price = product_info["base_price"]
    # Add realistic price variation (+/- 15% for discounts/markups)
    unit_price = round(base_price * random.uniform(0.85, 1.15), 2)
    total = round(unit_price * quantity, 2)
    order_date = start_date + timedelta(days=random.randint(0, date_range_days))

    record = {
        "order_id": f"ORD-{10000 + i}",
        "order_date": mess_up_date(order_date),
        "product_name": mess_up_product_name(product_name),
        "category": mess_up_category(product_info["category"]),
        "quantity": quantity,
        "unit_price": mess_up_price(unit_price),
        "total_amount": mess_up_price(total),
        "region": mess_up_region(random.choice(REGIONS)),
        "sales_rep": mess_up_rep_name(random.choice(SALES_REPS)),
        "customer_type": random.choice(CUSTOMER_TYPES),
        "payment_method": random.choice(PAYMENT_METHODS),
        "discount_pct": random.choice([0, 0, 0, 5, 10, 15, 20, ""]),  # Some blank
        "notes": random.choice([
            "", "", "", "", "",  # Most blank
            "Rush order", "Backordered", "Customer complaint",
            "VIP client", "Damaged in transit", "Return pending",
            "N/A", "n/a", "-", "none", "NULL",
        ]),
    }
    records.append(record)

# --- Inject Exact Duplicates ---
n_dupes = int(N_RECORDS * DUPLICATE_RATE)
dupes = random.sample(records, n_dupes)
records.extend(dupes)

# --- Inject Near-Duplicates (same order_id, slightly different data) ---
for _ in range(int(N_RECORDS * 0.03)):
    base = random.choice(records[:N_RECORDS]).copy()
    # Change one field slightly
    base["quantity"] = base["quantity"] + random.choice([-1, 1])
    records.append(base)

# --- Inject Nulls ---
df = pd.DataFrame(records)
for col in ["category", "region", "sales_rep", "discount_pct", "payment_method"]:
    mask = np.random.random(len(df)) < MISSING_RATE
    df.loc[mask, col] = np.nan

# --- Inject a few truly bad rows ---
bad_rows = pd.DataFrame([
    {
        "order_id": "", "order_date": "not a date", "product_name": "",
        "category": "", "quantity": "abc", "unit_price": "free",
        "total_amount": "", "region": "", "sales_rep": "",
        "customer_type": "", "payment_method": "", "discount_pct": "",
        "notes": "TEST ROW - IGNORE"
    },
    {
        "order_id": "ORD-99999", "order_date": "13/25/2024",
        "product_name": "Unknown Product", "category": "???",
        "quantity": -5, "unit_price": 0, "total_amount": 0,
        "region": "Narnia", "sales_rep": "Test User",
        "customer_type": "Internal", "payment_method": "Cash",
        "discount_pct": 150, "notes": "Bad data entry"
    },
    {
        "order_id": "ORD-10001", "order_date": "2025-06-15",
        "product_name": "Laptop Pro 15", "category": "Electronics",
        "quantity": 1, "unit_price": "$1,299.99", "total_amount": "$1,299.99",
        "region": "North", "sales_rep": "Maria Rodriguez",
        "customer_type": "Retail", "payment_method": "Credit Card",
        "discount_pct": 0, "notes": "Future date - should not exist"
    },
])
df = pd.concat([df, bad_rows], ignore_index=True)

# Shuffle
df = df.sample(frac=1, random_state=42).reset_index(drop=True)

# --- Save ---
output_path = os.path.join(os.path.dirname(__file__), "..", "data", "raw", "messy_sales_data.csv")
os.makedirs(os.path.dirname(output_path), exist_ok=True)
df.to_csv(output_path, index=False)

print(f"Generated {len(df)} records with realistic data quality issues.")
print(f"Saved to: {output_path}")
print(f"\nData quality summary:")
print(f"  Total records: {len(df)}")
print(f"  Exact duplicates injected: ~{n_dupes}")
print(f"  Near-duplicates injected: ~{int(N_RECORDS * 0.03)}")
print(f"  Null values injected: ~{MISSING_RATE*100:.0f}% per selected column")
print(f"  Invalid/test rows: 3")
print(f"  Inconsistent formats: dates, prices, names, regions, categories")

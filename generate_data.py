"""Create a messy sample retail sales CSV so the ETL has something to clean."""
import csv
import random
from datetime import date, timedelta

random.seed(42)
PRODUCTS = {
    "P100": ("Laptop", "Electronics", 55000),
    "P101": ("Headphones", "Electronics", 2500),
    "P102": ("Notebook", "Stationery", 60),
    "P103": ("Pen Pack", "Stationery", 120),
    "P104": ("Backpack", "Accessories", 1800),
    "P105": ("Water Bottle", "Accessories", 450),
}
CITIES = ["Hyderabad", "Guntur", "Vijayawada", "Warangal", "hyderabad", " Guntur "]

rows = []
start = date(2025, 1, 1)
for i in range(1, 2001):
    pid = random.choice(list(PRODUCTS))
    name, cat, price = PRODUCTS[pid]
    d = start + timedelta(days=random.randint(0, 364))
    qty = random.choice([1, 1, 1, 2, 3, 5, -1, None])  # some bad values on purpose
    rows.append([i, d.isoformat(), pid, name, cat, qty, price, random.choice(CITIES)])

# duplicate a few rows on purpose
rows += random.sample(rows, 25)

with open("sales_raw.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["order_id", "order_date", "product_id", "product_name",
                "category", "quantity", "unit_price", "city"])
    w.writerows(rows)
print(f"Wrote sales_raw.csv with {len(rows)} rows")

"""
Generates synthetic e-commerce order data as CSV.
Usage: python generate_orders.py [--date YYYY-MM-DD] [--rows 150000]
"""

import csv, random, uuid, argparse
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()
random.seed(42)

STATUSES   = ['completed', 'completed', 'completed', 'pending', 'refunded', 'cancelled']
CHANNELS   = ['organic', 'paid_search', 'email', 'social', 'direct', 'affiliate']
CURRENCIES = ['USD', 'EUR', 'GBP']
COUNTRIES  = ['US', 'GB', 'DE', 'FR', 'CA', 'AU', 'NL', 'ES']

CUSTOMER_IDS = [f"cust_{str(i).zfill(6)}" for i in range(1, 50_001)]
PRODUCT_IDS  = [f"prod_{str(i).zfill(4)}" for i in range(1, 501)]

def generate_orders(date_str, num_rows):
    date = datetime.strptime(date_str, "%Y-%m-%d")
    records = []
    for _ in range(num_rows):
        order_time = date + timedelta(
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )
        amount = round(random.lognormvariate(3.5, 1.2), 2)
        amount = max(1.0, min(amount, 5000.0))
        records.append({
            "order_id":        str(uuid.uuid4()),
            "customer_id":     random.choice(CUSTOMER_IDS),
            "product_id":      random.choice(PRODUCT_IDS),
            "order_date":      order_time.isoformat(),
            "order_status":    random.choice(STATUSES),
            "order_amount":    amount,
            "currency":        random.choice(CURRENCIES),
            "channel":         random.choice(CHANNELS),
            "discount_pct":    round(random.choice([0, 0, 0, 5, 10, 15, 20]), 0),
            "shipping_country": random.choice(COUNTRIES),
        })
    # Introduce ~0.5% duplicates — the pipeline must handle these
    records.extend(random.choices(records, k=int(num_rows * 0.005)))
    random.shuffle(records)
    return records

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=datetime.today().strftime("%Y-%m-%d"))
    parser.add_argument("--rows", type=int, default=150_000)
    args = parser.parse_args()

    print(f"Generating orders for {args.date}...")
    records = generate_orders(args.date, args.rows)
    output  = f"orders_{args.date}.csv"
    with open(output, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)
    print(f"  Written {len(records):,} rows → {output}")
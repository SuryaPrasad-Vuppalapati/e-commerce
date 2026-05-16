"""
Generates synthetic click/event data as CSV.
Usage: python generate_clicks.py [--date YYYY-MM-DD] [--rows 300000]
"""

import csv, random, uuid, argparse
from datetime import datetime, timedelta

random.seed(42)
EVENT_TYPES  = ['page_view', 'page_view', 'page_view', 'product_view', 'add_to_cart', 'checkout', 'purchase']
DEVICE_TYPES = ['desktop', 'mobile', 'tablet']
CUSTOMER_IDS = [f"cust_{str(i).zfill(6)}" for i in range(1, 50_001)]
PRODUCT_IDS  = [f"prod_{str(i).zfill(4)}" for i in range(1, 501)]
SESSION_IDS  = [str(uuid.uuid4()) for _ in range(200_000)]

def generate_clicks(date_str, num_rows):
    date = datetime.strptime(date_str, "%Y-%m-%d")
    records = []
    for _ in range(num_rows):
        event_time = date + timedelta(
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )
        event_type = random.choice(EVENT_TYPES)
        product_id = random.choice(PRODUCT_IDS) if event_type in ('product_view', 'add_to_cart', 'purchase') else ''
        records.append({
            "click_id":        str(uuid.uuid4()),
            "session_id":      random.choice(SESSION_IDS),
            "customer_id":     random.choice(CUSTOMER_IDS),
            "product_id":      product_id,
            "event_type":      event_type,
            "event_timestamp": event_time.isoformat(),
            "page_url":        f"https://shop.example.com/{event_type}/{random.randint(1, 1000)}",
            "referrer_url":    random.choice(['https://google.com', 'https://facebook.com', '', 'https://email.example.com']),
            "device_type":     random.choice(DEVICE_TYPES),
        })
    records.extend(random.choices(records, k=int(num_rows * 0.005)))
    random.shuffle(records)
    return records

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=datetime.today().strftime("%Y-%m-%d"))
    parser.add_argument("--rows", type=int, default=300_000)
    args = parser.parse_args()

    print(f"Generating clicks for {args.date}...")
    records = generate_clicks(args.date, args.rows)
    output  = f"clicks_{args.date}.csv"
    with open(output, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)
    print(f"  Written {len(records):,} rows → {output}")
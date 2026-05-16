"""
Generates synthetic web session data as CSV.
Usage: python generate_sessions.py [--date YYYY-MM-DD] [--rows 50000]
"""

import csv, random, uuid, argparse
from datetime import datetime, timedelta

random.seed(42)
UTM_SOURCES   = ['google', 'facebook', 'email', 'direct', 'bing', 'instagram', 'tiktok']
UTM_MEDIUMS   = ['cpc', 'organic', 'email', 'social', 'referral']
UTM_CAMPAIGNS = ['summer_sale', 'brand_awareness', 'retargeting', 'newsletter', '', '']
DEVICE_TYPES  = ['desktop', 'mobile', 'tablet']
COUNTRIES     = ['US', 'GB', 'DE', 'FR', 'CA', 'AU', 'NL', 'ES']
CUSTOMER_IDS  = [f"cust_{str(i).zfill(6)}" for i in range(1, 50_001)]

def generate_sessions(date_str, num_rows):
    date = datetime.strptime(date_str, "%Y-%m-%d")
    records = []
    for _ in range(num_rows):
        start = date + timedelta(hours=random.randint(0, 23), minutes=random.randint(0, 59))
        duration_secs = max(5, min(int(random.lognormvariate(5, 1.5)), 7200))
        end = start + timedelta(seconds=duration_secs)
        records.append({
            "session_id":    str(uuid.uuid4()),
            "customer_id":   random.choice(CUSTOMER_IDS),
            "session_start": start.isoformat(),
            "session_end":   end.isoformat(),
            "page_views":    random.randint(1, 30),
            "utm_source":    random.choice(UTM_SOURCES),
            "utm_medium":    random.choice(UTM_MEDIUMS),
            "utm_campaign":  random.choice(UTM_CAMPAIGNS),
            "device_type":   random.choice(DEVICE_TYPES),
            "country":       random.choice(COUNTRIES),
        })
    records.extend(random.choices(records, k=int(num_rows * 0.005)))
    random.shuffle(records)
    return records

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=datetime.today().strftime("%Y-%m-%d"))
    parser.add_argument("--rows", type=int, default=50_000)
    args = parser.parse_args()

    print(f"Generating sessions for {args.date}...")
    records = generate_sessions(args.date, args.rows)
    output  = f"sessions_{args.date}.csv"
    with open(output, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)
    print(f"  Written {len(records):,} rows → {output}")
"""
Generates and uploads all three datasets to S3, partitioned by date.

Usage:
  python upload_to_s3.py                                      # today only
  python upload_to_s3.py --date 2024-01-15                    # specific date
  python upload_to_s3.py --start-date 2024-01-01 --end-date 2024-01-30
"""

import os, io, csv, argparse, boto3
from datetime import datetime, timedelta
from dotenv import load_dotenv

import sys
sys.path.insert(0, os.path.dirname(__file__))
from generate_orders   import generate_orders
from generate_clicks   import generate_clicks
from generate_sessions import generate_sessions

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

S3_BUCKET = os.environ["S3_BUCKET_NAME"]

s3_client = boto3.client(
    "s3",
    aws_access_key_id     = os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"],
    region_name           = os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
)

def records_to_csv_bytes(records):
    if not records:
        return b""
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=records[0].keys())
    writer.writeheader()
    writer.writerows(records)
    return buf.getvalue().encode("utf-8")

def upload_date(date_str):
    print(f"\n── {date_str} ──────────────────────────────────────")
    datasets = [
        ("orders",   generate_orders(date_str,   150_000), f"orders_{date_str}.csv"),
        ("clicks",   generate_clicks(date_str,   300_000), f"clicks_{date_str}.csv"),
        ("sessions", generate_sessions(date_str,  50_000), f"sessions_{date_str}.csv"),
    ]
    for entity, records, filename in datasets:
        s3_key = f"raw/{entity}/date={date_str}/{filename}"
        data   = records_to_csv_bytes(records)
        s3_client.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=data, ContentType="text/csv")
        print(f"  ✓ {entity:10s}  {len(records):>8,} rows  ({len(data)/1_048_576:.1f} MB)")

def date_range(start, end):
    current = datetime.strptime(start, "%Y-%m-%d")
    end_dt  = datetime.strptime(end,   "%Y-%m-%d")
    while current <= end_dt:
        yield current.strftime("%Y-%m-%d")
        current += timedelta(days=1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    args = parser.parse_args()

    if args.start_date and args.end_date:
        dates = list(date_range(args.start_date, args.end_date))
    elif args.date:
        dates = [args.date]
    else:
        dates = [datetime.today().strftime("%Y-%m-%d")]

    print(f"Uploading {len(dates)} day(s) to s3://{S3_BUCKET}/raw/")
    for d in dates:
        upload_date(d)
    print(f"\n Done. Verify: aws s3 ls s3://{S3_BUCKET}/raw/ --recursive")
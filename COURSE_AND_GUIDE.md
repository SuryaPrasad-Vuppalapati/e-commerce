# End-to-End ELT Pipeline — E-Commerce Analytics
### A Complete Course & Build Guide

**Stack:** AWS S3 · Snowflake · dbt · Apache Airflow (Astro) · Power BI  
**Goal:** Ingest 500,000+ daily e-commerce events, transform them into a star schema, and serve CVR, LTV, and CAC dashboards with under 2-hour data latency.

---

## Table of Contents

1. [What You Are Building](#1-what-you-are-building)
2. [Concepts You Need to Know First](#2-concepts-you-need-to-know-first)
3. [Accounts & Tools to Install](#3-accounts--tools-to-install)
4. [Phase 1 — Project Structure](#phase-1--project-structure)
5. [Phase 2 — AWS S3 Setup](#phase-2--aws-s3-setup)
6. [Phase 3 — Snowflake Setup](#phase-3--snowflake-setup)
7. [Phase 4 — Synthetic Data Generation](#phase-4--synthetic-data-generation)
8. [Phase 5 — dbt Project Setup](#phase-5--dbt-project-setup)
9. [Phase 6 — dbt Staging Models](#phase-6--dbt-staging-models)
10. [Phase 7 — dbt Mart Models](#phase-7--dbt-mart-models)
11. [Phase 8 — dbt Tests & Data Quality](#phase-8--dbt-tests--data-quality)
12. [Phase 9 — Airflow (Astro) Orchestration](#phase-9--airflow-astro-orchestration)
13. [Phase 10 — Power BI Dashboards](#phase-10--power-bi-dashboards)
14. [Phase 11 — Running the Full Pipeline](#phase-11--running-the-full-pipeline)
15. [Troubleshooting](#troubleshooting)

---

## 1. What You Are Building

This is a **production-style ELT pipeline** — Extract, Load, Transform — for an e-commerce business. Unlike ETL (which transforms data before loading), ELT loads raw data first and transforms it inside the warehouse. This is the modern approach used at companies like Airbnb, GitLab, and Shopify.

**The data flow from start to finish:**

```
E-Commerce Sources          Raw event data: orders, clicks, sessions
        │
        ▼  (Python script — daily batch upload)
AWS S3 Landing Zone         s3://your-bucket/raw/orders/date=YYYY-MM-DD/
        │
        ▼  (Airflow triggers COPY INTO)
Snowflake — RAW schema      raw.orders_raw, raw.clicks_raw, raw.sessions_raw
        │
        ▼  (dbt staging models)
Snowflake — STAGING schema  stg_orders, stg_clicks, stg_sessions (cleaned, typed, deduped)
        │
        ▼  (dbt mart models)
Snowflake — MARTS schema    fact_orders, dim_customers, fct_daily_metrics (star schema)
        │
        ▼  (DirectQuery / scheduled refresh)
Power BI Dashboards         CVR funnel · LTV cohorts · CAC by channel
```

**Airflow** acts as the conductor — it runs on a daily schedule, waits for S3 files to arrive, kicks off the Snowflake loads, runs dbt, and sends an alert if anything fails.

---

## 2. Concepts You Need to Know First

Read this section carefully before touching any code. It explains the "why" behind every decision.

### 2.1 ELT vs ETL

Traditional **ETL** transforms data *before* loading it into the warehouse. This means your transformation logic lives in a separate system (often Java or Python pipelines), making it hard to debug and version-control.

**ELT** loads raw data first, then transforms it *inside* the warehouse using SQL. This is better because:
- Raw data is preserved — you can always re-transform it
- SQL is readable and version-controllable (via dbt)
- Cloud warehouses like Snowflake are powerful enough to handle transformations at scale

### 2.2 Why Snowflake over Redshift

Snowflake separates storage and compute. You pay for storage (very cheap) and only pay for compute when your warehouse is actually running a query. With `AUTO_SUSPEND = 60`, your warehouse goes to sleep after 60 seconds of idle time. For a personal project, your compute cost will be near-zero — you'll only burn credits during the few minutes each day when the pipeline runs.

Redshift keeps compute running continuously, which makes it expensive even for small projects.

### 2.3 What dbt does

dbt (data build tool) is a framework that turns SQL `SELECT` statements into tables and views in your warehouse. Instead of writing `CREATE TABLE AS SELECT...` yourself, you write a plain `SELECT`, and dbt handles the `CREATE OR REPLACE` wrapper, dependency ordering, and documentation.

dbt also gives you:
- **Tests** — assert that columns are not null, unique, or within expected ranges
- **Lineage** — a visual DAG showing how tables depend on each other
- **Documentation** — auto-generated data catalog from your YAML files

### 2.4 The three-layer architecture (Raw → Staging → Marts)

This is the most important design pattern in modern data engineering.

**RAW layer** — data lands exactly as it came from the source. No transformations. All columns are VARCHAR. This is your safety net: if a downstream transform breaks, you can always re-run from raw.

**STAGING layer** — one model per source table. This layer casts types (VARCHAR → TIMESTAMP, → DECIMAL), renames columns to a consistent standard, deduplicates rows, and flags data quality issues. Staging models are `views` — they don't store data, they just define the transformation logic.

**MARTS layer** — business-facing tables. These are the star schema fact and dimension tables that Power BI queries. Mart models are `tables` — they are materialized and stored so BI queries are fast.

### 2.5 Star schema

A star schema organises data into:
- **Fact tables** — one row per business event (an order, a session). Contains measures (revenue, quantity) and foreign keys to dimensions.
- **Dimension tables** — one row per entity (a customer, a product). Contains descriptive attributes.

```
dim_customers ──┐
                ├── fact_orders ──── dim_products
dim_date ───────┘
```

Power BI and other BI tools are optimised for star schemas. Queries are fast and the model is intuitive for business users.

### 2.6 What Airflow does

Airflow is a workflow scheduler. You write a **DAG** (Directed Acyclic Graph) in Python that defines tasks and their dependencies. Airflow runs the DAG on a schedule, handles retries on failure, and gives you a UI to monitor every run.

**Astronomer (Astro)** is a managed Airflow platform. Its CLI (`astro`) lets you run Airflow locally with a single command (`astro dev start`) using Docker, without any manual setup.

### 2.7 Key metrics this pipeline produces

| Metric | Formula | Business question answered |
|---|---|---|
| **CVR** (Conversion Rate) | `orders / sessions × 100` | What % of visitors actually buy? |
| **LTV** (Lifetime Value) | `SUM(revenue) per customer` | How much is each customer worth over time? |
| **CAC** (Customer Acquisition Cost) | `ad_spend / new_customers` | How much does it cost to acquire one customer? |
| **Bounce Rate** | `single-page sessions / total sessions × 100` | Are visitors leaving immediately? |
| **AOV** (Average Order Value) | `total_revenue / total_orders` | How much does each order generate on average? |

---

## 3. Accounts & Tools to Install

### 3.1 Sign up for these services (all free or free-trial)

| Service | What it does in this project | Free tier |
|---|---|---|
| [AWS](https://aws.amazon.com/free) | Hosts raw CSV files in S3 | 5 GB S3 storage free for 12 months |
| [Snowflake](https://signup.snowflake.com) | Cloud data warehouse — transforms and stores analytics data | $400 trial credit (30 days) |
| [Astronomer](https://www.astronomer.io/try-astro) | Runs Airflow locally via Docker | Free for local development |
| [Power BI Desktop](https://powerbi.microsoft.com/downloads) | Builds the dashboards | Free download (Windows); use web version on Mac |

### 3.2 Install local tools

```bash
# ── Verify Python 3.9+ ──────────────────────────────────────────────────────
python --version

# ── AWS CLI ─────────────────────────────────────────────────────────────────
pip install awscli

# ── dbt with Snowflake adapter ───────────────────────────────────────────────
pip install dbt-snowflake

# ── Python libraries for data generation ─────────────────────────────────────
pip install boto3 faker pandas python-dotenv

# ── Astro CLI (Airflow) ───────────────────────────────────────────────────────
# macOS:
brew install astro

# Windows (PowerShell as Administrator):
winget install -e --id Astronomer.Astro

# ── Verify all tools installed ────────────────────────────────────────────────
aws --version
dbt --version
astro version
```

### 3.3 Configure AWS credentials

```bash
aws configure
# Prompts you to enter:
# AWS Access Key ID:     (from your IAM user — see Phase 2)
# AWS Secret Access Key: (from your IAM user)
# Default region:        us-east-1
# Default output format: json

# Test the connection:
aws sts get-caller-identity
# Should print your account ID and user ARN
```

---

## Phase 1 — Project Structure

### What this phase does

Creates the folder layout for the entire project. You do this once at the start and then fill in the files over the following phases.

### Step 1.1 — Create the project root

```bash
mkdir ecommerce_elt_pipeline
cd ecommerce_elt_pipeline
```

### Step 1.2 — Full folder structure

Create each folder now. The files inside them are created in later phases.

```
ecommerce_elt_pipeline/
├── .env                              ← your secrets (never commit to git)
├── .gitignore
│
├── data_generator/                   ← Phase 4: Python scripts to make fake data
│   ├── generate_orders.py
│   ├── generate_clicks.py
│   ├── generate_sessions.py
│   └── upload_to_s3.py
│
├── snowflake_setup/                  ← Phase 3: SQL run once in Snowflake UI
│   ├── 01_account_setup.sql
│   ├── 02_schemas_tables.sql
│   └── 03_stages.sql
│
├── dbt_project/                      ← Phases 5–8: all dbt code
│   ├── dbt_project.yml
│   ├── profiles.yml                  ← your Snowflake credentials (never commit)
│   ├── packages.yml
│   ├── models/
│   │   ├── staging/
│   │   │   ├── _staging__sources.yml
│   │   │   ├── stg_orders.sql
│   │   │   ├── stg_clicks.sql
│   │   │   └── stg_sessions.sql
│   │   └── marts/
│   │       ├── _marts__models.yml
│   │       ├── fact_orders.sql
│   │       ├── dim_customers.sql
│   │       ├── dim_products.sql
│   │       └── fct_daily_metrics.sql
│   └── tests/
│       └── assert_no_future_orders.sql
│
└── airflow_project/                  ← Phase 9: Airflow DAG and SQL
    ├── dags/
    │   └── ecommerce_elt_dag.py
    ├── include/
    │   └── sql/
    │       ├── load_orders.sql
    │       ├── load_clicks.sql
    │       └── load_sessions.sql
    └── requirements.txt
```

```bash
# Create all folders at once:
mkdir -p data_generator snowflake_setup \
  dbt_project/models/staging \
  dbt_project/models/marts \
  dbt_project/tests \
  airflow_project/dags \
  airflow_project/include/sql
```

### Step 1.3 — Create `.gitignore`

Create a file named `.gitignore` in the project root with this content:

```gitignore
# Credentials — never commit these
.env
profiles.yml
*.key

# Python
__pycache__/
*.pyc
.venv/
venv/

# dbt generated files
dbt_project/target/
dbt_project/logs/
dbt_project/.user.yml

# Airflow
airflow_project/logs/
airflow_project/.astro/
```

### Step 1.4 — Create `.env`

Create a file named `.env` in the project root. You will fill in the values as you go through the phases.

```env
# AWS — fill in after Phase 2
AWS_ACCESS_KEY_ID=your_key_here
AWS_SECRET_ACCESS_KEY=your_secret_here
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET_NAME=your-ecommerce-raw-data

# Snowflake — fill in after Phase 3
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=ECOMMERCE_DB
SNOWFLAKE_ROLE=SYSADMIN
```

> **Finding your Snowflake account identifier:** In Snowflake, click your name in the top-left → Account → copy the account locator. It looks like `abc12345.us-east-1`. Use that full string — do not add `.snowflakecomputing.com`.

---

## Phase 2 — AWS S3 Setup

### What this phase does

Creates an S3 bucket (your raw data landing zone) and an IAM user with the permissions to read and write to it. S3 acts as the buffer between your source data and Snowflake — files land here first, then Snowflake pulls them in.

### Step 2.1 — Create an S3 bucket

1. Sign into [AWS Console](https://console.aws.amazon.com) → navigate to **S3** → click **Create bucket**
2. Set these values:
   - **Bucket name:** `your-ecommerce-raw-data-YOURINITIALS` (must be globally unique across all AWS accounts — add your initials or a random suffix)
   - **Region:** `us-east-1` (or your closest region)
   - **Block all public access:** ✅ keep this checked — the bucket must be private
   - Leave all other settings as default → click **Create bucket**
3. Copy the bucket name into your `.env` file as `S3_BUCKET_NAME`

### Step 2.2 — Create an IAM user

IAM (Identity and Access Management) controls who can access your AWS resources. You need a user with programmatic access so your Python scripts can upload files.

1. In AWS Console → navigate to **IAM** → **Users** → **Create user**
2. Username: `ecommerce-pipeline-user`
3. On the **Set permissions** page → choose **Attach policies directly** → search for and attach `AmazonS3FullAccess`
4. Click through to **Create user**
5. Click on the newly created user → **Security credentials** tab → **Create access key** → choose **Local code** → **Create**
6. **Download the CSV** or copy the Access Key ID and Secret Access Key into your `.env` file now — you cannot see the secret again after closing this page

**For production (tighter permissions):** instead of `AmazonS3FullAccess`, create a custom policy that only allows access to your specific bucket:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:DeleteObject"],
      "Resource": [
        "arn:aws:s3:::your-ecommerce-raw-data-YOURINITIALS",
        "arn:aws:s3:::your-ecommerce-raw-data-YOURINITIALS/*"
      ]
    }
  ]
}
```

### Step 2.3 — Configure and test AWS CLI

```bash
aws configure
# Enter your Access Key ID, Secret, region (us-east-1), and output format (json)

# Verify it works:
aws s3 ls
# Should list your buckets without an error

# Verify your specific bucket is accessible:
aws s3 ls s3://your-ecommerce-raw-data-YOURINITIALS/
# Returns empty (no files yet) — that's correct
```

### Step 2.4 — Understand the S3 folder structure

Your upload scripts will automatically create date-partitioned folders:

```
s3://your-bucket/
└── raw/
    ├── orders/
    │   └── date=2024-01-15/
    │       └── orders_2024-01-15.csv
    ├── clicks/
    │   └── date=2024-01-15/
    │       └── clicks_2024-01-15.csv
    └── sessions/
        └── date=2024-01-15/
            └── sessions_2024-01-15.csv
```

The `date=YYYY-MM-DD` partition pattern is a Hive-style convention that Snowflake external stages understand natively. Airflow passes the execution date when triggering the COPY INTO command, so each day's run loads only that day's files.

---

## Phase 3 — Snowflake Setup

### What this phase does

Sets up the Snowflake objects your pipeline needs: a warehouse (compute), a database, three schemas (RAW / STAGING / MARTS), the raw landing tables, and an external stage that points to your S3 bucket.

You run these SQL scripts **once** in the Snowflake web UI. They are idempotent (`IF NOT EXISTS`) so you can re-run them safely.

### Step 3.1 — Log into Snowflake

Go to [app.snowflake.com](https://app.snowflake.com) and sign in. Click **Worksheets** in the left sidebar → **+** to open a new worksheet.

### Step 3.2 — Run `01_account_setup.sql`

Copy this into a Snowflake worksheet and run it (Ctrl+Enter or the Run button):

```sql
-- Run as ACCOUNTADMIN — creates the database, warehouse, and pipeline role

USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND   = 60
    AUTO_RESUME    = TRUE
    COMMENT        = 'ELT pipeline compute warehouse';

CREATE DATABASE IF NOT EXISTS ECOMMERCE_DB
    COMMENT = 'E-commerce ELT pipeline database';

CREATE ROLE IF NOT EXISTS PIPELINE_ROLE
    COMMENT = 'Role for Airflow + dbt pipeline operations';

GRANT USAGE ON WAREHOUSE COMPUTE_WH  TO ROLE PIPELINE_ROLE;
GRANT USAGE ON DATABASE  ECOMMERCE_DB TO ROLE PIPELINE_ROLE;
GRANT ALL   ON DATABASE  ECOMMERCE_DB TO ROLE PIPELINE_ROLE;
GRANT CREATE SCHEMA ON DATABASE ECOMMERCE_DB TO ROLE PIPELINE_ROLE;
GRANT ROLE PIPELINE_ROLE TO ROLE SYSADMIN;

USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE ECOMMERCE_DB;
```

> **What `AUTO_SUSPEND = 60` means:** The warehouse (compute engine) pauses automatically after 60 seconds of no queries. Since Snowflake charges for compute per second, this is critical for keeping costs near-zero on a personal project. `AUTO_RESUME = TRUE` means it wakes up automatically when a query arrives.

### Step 3.3 — Run `02_schemas_tables.sql`

```sql
-- Creates RAW, STAGING, MARTS schemas and the raw landing tables

USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE ECOMMERCE_DB;

CREATE SCHEMA IF NOT EXISTS RAW     COMMENT = 'Raw data loaded from S3';
CREATE SCHEMA IF NOT EXISTS STAGING COMMENT = 'Cleaned by dbt staging models';
CREATE SCHEMA IF NOT EXISTS MARTS   COMMENT = 'Star schema for BI consumption';

GRANT ALL ON SCHEMA ECOMMERCE_DB.RAW     TO ROLE PIPELINE_ROLE;
GRANT ALL ON SCHEMA ECOMMERCE_DB.STAGING TO ROLE PIPELINE_ROLE;
GRANT ALL ON SCHEMA ECOMMERCE_DB.MARTS   TO ROLE PIPELINE_ROLE;

GRANT ALL ON FUTURE TABLES IN SCHEMA ECOMMERCE_DB.RAW     TO ROLE PIPELINE_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA ECOMMERCE_DB.STAGING TO ROLE PIPELINE_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA ECOMMERCE_DB.MARTS   TO ROLE PIPELINE_ROLE;

USE SCHEMA RAW;

-- All columns are VARCHAR in the raw layer — we cast types in dbt staging
CREATE TABLE IF NOT EXISTS ORDERS_RAW (
    order_id          VARCHAR(50),
    customer_id       VARCHAR(50),
    product_id        VARCHAR(50),
    order_date        VARCHAR(30),
    order_status      VARCHAR(30),
    order_amount      VARCHAR(20),
    currency          VARCHAR(10),
    channel           VARCHAR(50),
    discount_pct      VARCHAR(10),
    shipping_country  VARCHAR(50),
    _s3_file_path     VARCHAR(500),
    _loaded_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS CLICKS_RAW (
    click_id          VARCHAR(50),
    session_id        VARCHAR(50),
    customer_id       VARCHAR(50),
    product_id        VARCHAR(50),
    event_type        VARCHAR(50),
    event_timestamp   VARCHAR(30),
    page_url          VARCHAR(500),
    referrer_url      VARCHAR(500),
    device_type       VARCHAR(30),
    _s3_file_path     VARCHAR(500),
    _loaded_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS SESSIONS_RAW (
    session_id        VARCHAR(50),
    customer_id       VARCHAR(50),
    session_start     VARCHAR(30),
    session_end       VARCHAR(30),
    page_views        VARCHAR(10),
    utm_source        VARCHAR(100),
    utm_medium        VARCHAR(100),
    utm_campaign      VARCHAR(200),
    device_type       VARCHAR(30),
    country           VARCHAR(50),
    _s3_file_path     VARCHAR(500),
    _loaded_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Verify:
SHOW TABLES IN SCHEMA RAW;
-- You should see: ORDERS_RAW, CLICKS_RAW, SESSIONS_RAW
```

> **Why all VARCHAR?** The raw layer is a faithful copy of the source file. If a source system sends `order_amount = "N/A"` instead of a number, you don't want the load to fail — you want to load it and handle the error in dbt. `TRY_TO_DECIMAL()` in the staging model converts gracefully and returns NULL for bad values instead of crashing.

### Step 3.4 — Run `03_stages.sql`

An **external stage** is a Snowflake object that knows how to read files from S3. Once created, you can run `COPY INTO` commands against it without re-specifying credentials each time.

Replace the four placeholders before running:

```sql
-- Replace <YOUR_BUCKET_NAME>, <YOUR_AWS_KEY_ID>, <YOUR_AWS_SECRET>, <YOUR_REGION>

USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE ECOMMERCE_DB;
USE SCHEMA RAW;

CREATE OR REPLACE FILE FORMAT CSV_FORMAT
    TYPE                      = 'CSV'
    FIELD_DELIMITER           = ','
    RECORD_DELIMITER          = '\n'
    SKIP_HEADER               = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF                   = ('NULL', 'null', '', 'N/A')
    EMPTY_FIELD_AS_NULL       = TRUE
    TRIM_SPACE                = TRUE;

CREATE OR REPLACE STAGE S3_RAW_STAGE
    URL         = 's3://<YOUR_BUCKET_NAME>/raw/'
    CREDENTIALS = (
        AWS_KEY_ID     = '<YOUR_AWS_KEY_ID>'
        AWS_SECRET_KEY = '<YOUR_AWS_SECRET>'
    )
    FILE_FORMAT = CSV_FORMAT;

-- Test: Snowflake should be able to list your S3 files
LIST @S3_RAW_STAGE;
-- If you see files, the stage works. If you see an error, check your credentials.
```

---

## Phase 4 — Synthetic Data Generation

### What this phase does

Since you don't have a real e-commerce system, you generate synthetic data that realistically mimics what a real system would produce — including deliberate duplicates (~0.5% of rows) that the pipeline's deduplication logic must handle.

### Step 4.1 — Create `data_generator/generate_orders.py`

```python
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
```

### Step 4.2 — Create `data_generator/generate_clicks.py`

```python
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
```

### Step 4.3 — Create `data_generator/generate_sessions.py`

```python
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
```

### Step 4.4 — Create `data_generator/upload_to_s3.py`

This script generates all three datasets and uploads them to S3 in one go. It supports both single-date and date-range modes, which you use for backfilling historical data.

```python
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
    print(f"\n✅ Done. Verify: aws s3 ls s3://{S3_BUCKET}/raw/ --recursive")
```

### Step 4.5 — Run the upload

```bash
cd data_generator

# Upload today's data (single run):
python upload_to_s3.py

# Or backfill 30 days of history (recommended for richer Power BI visuals):
python upload_to_s3.py --start-date 2024-01-01 --end-date 2024-01-30

# Verify files are in S3:
aws s3 ls s3://your-ecommerce-raw-data-YOURINITIALS/raw/orders/ --recursive
```

---

## Phase 5 — dbt Project Setup

### What this phase does

Initialises the dbt project and connects it to Snowflake. You only do this once.

### Step 5.1 — Create `dbt_project/dbt_project.yml`

This is dbt's main configuration file. It tells dbt where your models live and how to materialise them.

```yaml
name: 'ecommerce_elt'
version: '1.0.0'
config-version: 2

profile: 'ecommerce_elt'    # must match the name in profiles.yml

model-paths: ["models"]
test-paths:  ["tests"]
log-path:    "logs"
target-path: "target"

models:
  ecommerce_elt:
    staging:
      +schema:       staging         # creates tables in ECOMMERCE_DB.STAGING
      +materialized: view            # staging models are views — no storage cost

    marts:
      +schema:       marts           # creates tables in ECOMMERCE_DB.MARTS
      +materialized: table           # mart models are tables — fast for BI

vars:
  start_date: '2024-01-01'
```

### Step 5.2 — Create `dbt_project/packages.yml`

dbt packages add extra test capabilities. `dbt_expectations` gives you tests like "expect column values to be between X and Y".

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.0.0", "<2.0.0"]

  - package: calogica/dbt_expectations
    version: [">=0.9.0", "<1.0.0"]
```

### Step 5.3 — Create `dbt_project/profiles.yml`

This file holds your Snowflake credentials. Place it at `~/.dbt/profiles.yml` (dbt's default) or in the `dbt_project/` folder. **Do not commit it to git.**

```yaml
ecommerce_elt:
  target: dev
  outputs:
    dev:
      type:      snowflake
      account:   "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user:      "{{ env_var('SNOWFLAKE_USER') }}"
      password:  "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role:      SYSADMIN
      database:  ECOMMERCE_DB
      warehouse: COMPUTE_WH
      schema:    RAW
      threads:   4

    prod:
      type:      snowflake
      account:   "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user:      "{{ env_var('SNOWFLAKE_USER') }}"
      password:  "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role:      PIPELINE_ROLE
      database:  ECOMMERCE_DB
      warehouse: COMPUTE_WH
      schema:    RAW
      threads:   8
```

### Step 5.4 — Install packages and test the connection

```bash
cd dbt_project

# Install the packages from packages.yml
dbt deps

# Test that dbt can connect to Snowflake
dbt debug
# Expected output: "All checks passed!"
# If it fails, recheck your SNOWFLAKE_ACCOUNT value in .env (no .snowflakecomputing.com suffix)
```

---

## Phase 6 — dbt Staging Models

### What staging models do

Staging models sit directly on top of raw tables. Each one does exactly four things:

1. **Select** from the raw source table
2. **Cast types** — VARCHAR → TIMESTAMP, DECIMAL, INTEGER
3. **Rename columns** to a consistent naming convention
4. **Deduplicate** using `ROW_NUMBER()` — keeps the latest loaded version of each record by its primary key

Staging models are materialised as **views** — they cost nothing to store and always reflect the latest raw data.

### Step 6.1 — Create `models/staging/_staging__sources.yml`

This YAML file registers your raw tables as official dbt sources. This is how dbt tracks lineage — it knows `stg_orders` depends on `raw.orders_raw`, which came from S3.

```yaml
version: 2

sources:
  - name: raw
    database: ECOMMERCE_DB
    schema:   RAW
    description: "Raw e-commerce data loaded from AWS S3"
    freshness:
      warn_after:  {count: 25, period: hour}
      error_after: {count: 49, period: hour}
    loaded_at_field: _loaded_at

    tables:
      - name: orders_raw
        description: "Raw order transactions from all sales channels"
        columns:
          - name: order_id
            tests: [not_null]
          - name: customer_id
            tests: [not_null]
          - name: order_date
            tests: [not_null]
          - name: order_status
            tests:
              - accepted_values:
                  values: ['completed', 'pending', 'refunded', 'cancelled']

      - name: clicks_raw
        description: "Raw click and page event stream"
        columns:
          - name: click_id
            tests: [not_null]
          - name: event_type
            tests:
              - accepted_values:
                  values: ['page_view', 'product_view', 'add_to_cart', 'checkout', 'purchase']

      - name: sessions_raw
        description: "Raw web session data with UTM attribution"
        columns:
          - name: session_id
            tests: [not_null]
          - name: session_start
            tests: [not_null]
```

### Step 6.2 — Create `models/staging/stg_orders.sql`

```sql
-- Cleans, types, and deduplicates raw orders.
-- Materialised as a VIEW.

WITH

source AS (
    SELECT * FROM {{ source('raw', 'orders_raw') }}
),

typed AS (
    SELECT
        order_id,
        customer_id,
        product_id,
        TRY_TO_TIMESTAMP(order_date)                          AS order_timestamp,
        DATE(TRY_TO_TIMESTAMP(order_date))                    AS order_date,
        UPPER(TRIM(order_status))                             AS order_status,
        COALESCE(TRY_TO_DECIMAL(order_amount, 10, 2), 0.00)  AS order_amount_usd,
        UPPER(TRIM(currency))                                 AS currency,
        LOWER(TRIM(channel))                                  AS acquisition_channel,
        COALESCE(TRY_TO_NUMBER(discount_pct), 0)              AS discount_pct,
        UPPER(TRIM(shipping_country))                         AS shipping_country,
        _s3_file_path,
        _loaded_at
    FROM source
    WHERE order_id IS NOT NULL
),

-- Keep only the latest loaded version of each order_id (deduplication)
deduped AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY order_id
                ORDER BY _loaded_at DESC
            ) AS _row_num
        FROM typed
    )
    WHERE _row_num = 1
),

final AS (
    SELECT
        order_id,
        customer_id,
        product_id,
        order_timestamp,
        order_date,
        order_status,
        order_amount_usd,
        currency,
        acquisition_channel,
        discount_pct,
        shipping_country,
        _s3_file_path,
        _loaded_at,
        ROUND(order_amount_usd * (1 - discount_pct / 100.0), 2) AS net_amount_usd,
        CASE
            WHEN order_amount_usd <= 0               THEN 'zero_or_negative_amount'
            WHEN order_timestamp  >  CURRENT_TIMESTAMP() THEN 'future_order'
            WHEN customer_id      IS NULL             THEN 'missing_customer'
            ELSE 'valid'
        END                                                   AS data_quality_flag
    FROM deduped
)

SELECT * FROM final
```

### Step 6.3 — Create `models/staging/stg_clicks.sql`

```sql
WITH source AS (
    SELECT * FROM {{ source('raw', 'clicks_raw') }}
),

typed AS (
    SELECT
        click_id,
        session_id,
        customer_id,
        NULLIF(TRIM(product_id), '')                          AS product_id,
        LOWER(TRIM(event_type))                               AS event_type,
        TRY_TO_TIMESTAMP(event_timestamp)                     AS event_timestamp,
        DATE(TRY_TO_TIMESTAMP(event_timestamp))               AS event_date,
        NULLIF(TRIM(page_url), '')                            AS page_url,
        NULLIF(TRIM(referrer_url), '')                        AS referrer_url,
        LOWER(TRIM(device_type))                              AS device_type,
        _s3_file_path,
        _loaded_at
    FROM source
    WHERE click_id IS NOT NULL
),

deduped AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY click_id
                ORDER BY _loaded_at DESC
            ) AS _row_num
        FROM typed
    )
    WHERE _row_num = 1
),

final AS (
    SELECT
        click_id,
        session_id,
        customer_id,
        product_id,
        event_type,
        event_timestamp,
        event_date,
        page_url,
        referrer_url,
        device_type,
        _s3_file_path,
        _loaded_at,
        CASE event_type
            WHEN 'page_view'    THEN 1
            WHEN 'product_view' THEN 2
            WHEN 'add_to_cart'  THEN 3
            WHEN 'checkout'     THEN 4
            WHEN 'purchase'     THEN 5
            ELSE 0
        END AS funnel_stage
    FROM deduped
)

SELECT * FROM final
```

### Step 6.4 — Create `models/staging/stg_sessions.sql`

```sql
WITH source AS (
    SELECT * FROM {{ source('raw', 'sessions_raw') }}
),

typed AS (
    SELECT
        session_id,
        customer_id,
        TRY_TO_TIMESTAMP(session_start)           AS session_start,
        TRY_TO_TIMESTAMP(session_end)             AS session_end,
        DATE(TRY_TO_TIMESTAMP(session_start))     AS session_date,
        COALESCE(TRY_TO_NUMBER(page_views), 0)   AS page_views,
        NULLIF(LOWER(TRIM(utm_source)), '')       AS utm_source,
        NULLIF(LOWER(TRIM(utm_medium)), '')       AS utm_medium,
        NULLIF(LOWER(TRIM(utm_campaign)), '')     AS utm_campaign,
        LOWER(TRIM(device_type))                  AS device_type,
        UPPER(TRIM(country))                      AS country,
        _s3_file_path,
        _loaded_at
    FROM source
    WHERE session_id IS NOT NULL
),

deduped AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY session_id
                ORDER BY _loaded_at DESC
            ) AS _row_num
        FROM typed
    )
    WHERE _row_num = 1
),

final AS (
    SELECT
        session_id,
        customer_id,
        session_start,
        session_end,
        session_date,
        page_views,
        utm_source,
        utm_medium,
        utm_campaign,
        device_type,
        country,
        _s3_file_path,
        _loaded_at,
        DATEDIFF('second', session_start, session_end)  AS session_duration_secs,
        CASE
            WHEN page_views <= 1
             AND DATEDIFF('second', session_start, session_end) < 10
            THEN TRUE ELSE FALSE
        END AS is_bounce,
        CASE
            WHEN utm_medium = 'cpc'     THEN 'paid_search'
            WHEN utm_medium = 'social'  THEN 'paid_social'
            WHEN utm_medium = 'email'   THEN 'email'
            WHEN utm_medium = 'organic' THEN 'organic_search'
            WHEN utm_source = 'direct'  THEN 'direct'
            WHEN utm_medium = 'referral' THEN 'referral'
            ELSE 'other'
        END AS channel_group
    FROM deduped
    WHERE session_start IS NOT NULL
)

SELECT * FROM final
```

### Step 6.5 — Run staging models

```bash
cd dbt_project

# Run only staging models
dbt run --select staging

# Check the output in Snowflake:
# ECOMMERCE_DB.STAGING.STG_ORDERS  (view)
# ECOMMERCE_DB.STAGING.STG_CLICKS  (view)
# ECOMMERCE_DB.STAGING.STG_SESSIONS (view)
```

---

## Phase 7 — dbt Mart Models

### What mart models do

Mart models build the star schema that Power BI queries. They join staging models together, add business logic, and materialise as **tables** (stored on disk for fast query performance).

The star schema for this project:

```
dim_customers ──┐
                ├── fact_orders ──── dim_products
                │
                └── fct_daily_metrics  (pre-aggregated KPIs)
```

### Step 7.1 — Create `models/marts/fact_orders.sql`

The fact table. One row per order, joined to dimension tables.

```sql
{{
  config(
    materialized = 'table',
    cluster_by   = ['order_date']
  )
}}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
    WHERE data_quality_flag = 'valid'
),

products AS (
    SELECT product_id, product_category, product_name
    FROM {{ ref('dim_products') }}
),

first_orders AS (
    SELECT customer_id, MIN(order_date) AS first_order_date
    FROM {{ ref('stg_orders') }}
    WHERE data_quality_flag = 'valid'
    GROUP BY 1
)

SELECT
    o.order_id,
    o.customer_id,
    o.product_id,
    o.order_date,
    o.order_timestamp,
    o.order_status,
    o.order_amount_usd,
    o.discount_pct,
    o.net_amount_usd,
    o.currency,
    o.acquisition_channel,
    o.shipping_country,
    p.product_category,
    p.product_name,
    CASE
        WHEN o.order_date = f.first_order_date THEN TRUE
        ELSE FALSE
    END AS is_first_order,
    o._loaded_at

FROM orders o
LEFT JOIN products     p ON o.product_id  = p.product_id
LEFT JOIN first_orders f ON o.customer_id = f.customer_id
```

### Step 7.2 — Create `models/marts/dim_customers.sql`

Customer dimension with lifetime value and segment classification.

```sql
{{ config(materialized='table') }}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
    WHERE data_quality_flag = 'valid'
),

customer_orders AS (
    SELECT
        customer_id,
        MIN(order_date)     AS first_order_date,
        MAX(order_date)     AS last_order_date,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(net_amount_usd) AS lifetime_value_usd,
        AVG(net_amount_usd) AS avg_order_value_usd,
        FIRST_VALUE(acquisition_channel) OVER (
            PARTITION BY customer_id ORDER BY order_date ASC
        ) AS acquisition_channel
    FROM orders
    GROUP BY customer_id
)

SELECT
    customer_id,
    acquisition_channel,
    first_order_date,
    last_order_date,
    total_orders,
    ROUND(lifetime_value_usd, 2)     AS lifetime_value_usd,
    ROUND(avg_order_value_usd, 2)    AS avg_order_value_usd,
    DATE_TRUNC('month', first_order_date) AS acquisition_cohort_month,
    DATEDIFF('day', last_order_date, CURRENT_DATE()) AS days_since_last_order,
    CASE
        WHEN lifetime_value_usd >= 1000 THEN 'high_value'
        WHEN lifetime_value_usd >= 200  THEN 'mid_value'
        ELSE 'low_value'
    END AS customer_segment

FROM customer_orders
```

### Step 7.3 — Create `models/marts/dim_products.sql`

Product dimension. In production this would join a real product catalog — here it derives attributes from the product ID.

```sql
{{ config(materialized='table') }}

WITH orders AS (
    SELECT DISTINCT product_id FROM {{ ref('stg_orders') }}
    WHERE product_id IS NOT NULL
)

SELECT
    product_id,
    'Product ' || UPPER(SUBSTR(product_id, 6))           AS product_name,
    CASE
        WHEN RIGHT(product_id, 3)::INT BETWEEN 1   AND 100 THEN 'Electronics'
        WHEN RIGHT(product_id, 3)::INT BETWEEN 101 AND 200 THEN 'Clothing'
        WHEN RIGHT(product_id, 3)::INT BETWEEN 201 AND 300 THEN 'Home & Garden'
        WHEN RIGHT(product_id, 3)::INT BETWEEN 301 AND 400 THEN 'Sports'
        ELSE 'Other'
    END AS product_category,
    ROUND(10 + (RIGHT(product_id, 3)::INT * 0.5), 2)    AS base_price_usd
FROM orders
```

### Step 7.4 — Create `models/marts/fct_daily_metrics.sql`

This is the most important table for Power BI. It pre-aggregates CVR, LTV inputs, and CAC at the daily × channel level, so dashboards load instantly without expensive real-time joins.

```sql
{{
  config(
    materialized = 'table',
    cluster_by   = ['metric_date']
  )
}}

WITH

sessions_daily AS (
    SELECT
        session_date          AS metric_date,
        channel_group,
        COUNT(DISTINCT session_id)          AS total_sessions,
        COUNT(DISTINCT customer_id)         AS unique_visitors,
        SUM(CASE WHEN is_bounce THEN 1 ELSE 0 END) AS bounced_sessions,
        AVG(session_duration_secs)          AS avg_session_duration_secs
    FROM {{ ref('stg_sessions') }}
    GROUP BY 1, 2
),

clicks_daily AS (
    SELECT
        event_date            AS metric_date,
        COUNT(DISTINCT CASE WHEN event_type = 'add_to_cart' THEN session_id END) AS add_to_cart_sessions,
        COUNT(DISTINCT CASE WHEN event_type = 'checkout'    THEN session_id END) AS checkout_sessions,
        COUNT(DISTINCT CASE WHEN event_type = 'purchase'    THEN session_id END) AS purchase_sessions
    FROM {{ ref('stg_clicks') }}
    GROUP BY 1
),

orders_daily AS (
    SELECT
        order_date            AS metric_date,
        acquisition_channel   AS channel_group,
        COUNT(DISTINCT order_id)            AS total_orders,
        COUNT(DISTINCT customer_id)         AS purchasing_customers,
        COUNT(DISTINCT CASE WHEN is_first_order THEN customer_id END) AS new_customers,
        SUM(net_amount_usd)                 AS total_revenue_usd,
        AVG(net_amount_usd)                 AS avg_order_value_usd,
        SUM(CASE WHEN order_status = 'refunded' THEN net_amount_usd ELSE 0 END) AS refund_amount_usd
    FROM {{ ref('fact_orders') }}
    GROUP BY 1, 2
),

combined AS (
    SELECT
        s.metric_date,
        s.channel_group,
        s.total_sessions,
        s.unique_visitors,
        s.bounced_sessions,
        ROUND(s.avg_session_duration_secs, 0)   AS avg_session_duration_secs,
        c.add_to_cart_sessions,
        c.checkout_sessions,
        c.purchase_sessions,
        COALESCE(o.total_orders, 0)             AS total_orders,
        COALESCE(o.new_customers, 0)            AS new_customers,
        COALESCE(o.total_revenue_usd, 0)        AS total_revenue_usd,
        COALESCE(o.avg_order_value_usd, 0)      AS avg_order_value_usd,
        COALESCE(o.refund_amount_usd, 0)        AS refund_amount_usd
    FROM sessions_daily s
    LEFT JOIN clicks_daily c ON s.metric_date   = c.metric_date
    LEFT JOIN orders_daily o ON s.metric_date   = o.metric_date
                             AND s.channel_group = o.channel_group
)

SELECT
    metric_date,
    channel_group,
    total_sessions,
    unique_visitors,
    bounced_sessions,
    avg_session_duration_secs,
    add_to_cart_sessions,
    checkout_sessions,
    purchase_sessions,
    total_orders,
    new_customers,
    total_revenue_usd,
    avg_order_value_usd,
    refund_amount_usd,

    -- CVR: sessions that ended in a purchase
    ROUND(
        CASE WHEN total_sessions > 0
             THEN (purchase_sessions::FLOAT / total_sessions) * 100
             ELSE 0 END, 2
    ) AS cvr_pct,

    -- Add-to-cart rate
    ROUND(
        CASE WHEN total_sessions > 0
             THEN (add_to_cart_sessions::FLOAT / total_sessions) * 100
             ELSE 0 END, 2
    ) AS add_to_cart_rate_pct,

    -- Bounce rate
    ROUND(
        CASE WHEN total_sessions > 0
             THEN (bounced_sessions::FLOAT / total_sessions) * 100
             ELSE 0 END, 2
    ) AS bounce_rate_pct,

    -- Revenue per session
    ROUND(
        CASE WHEN total_sessions > 0
             THEN total_revenue_usd / total_sessions
             ELSE 0 END, 2
    ) AS revenue_per_session_usd,

    -- CAC placeholder — connect ad spend data to fill this in
    0.00 AS ad_spend_usd,
    0.00 AS cac_usd

FROM combined
ORDER BY metric_date DESC, total_revenue_usd DESC
```

### Step 7.5 — Run mart models

```bash
# Run all marts (dbt resolves dependencies automatically — dim tables run before fact)
dbt run --select marts

# Verify in Snowflake:
# ECOMMERCE_DB.MARTS.FACT_ORDERS
# ECOMMERCE_DB.MARTS.DIM_CUSTOMERS
# ECOMMERCE_DB.MARTS.DIM_PRODUCTS
# ECOMMERCE_DB.MARTS.FCT_DAILY_METRICS
```

---

## Phase 8 — dbt Tests & Data Quality

### What dbt tests do

dbt tests are SQL queries that must return zero rows to pass. If they return rows, the test fails — and Airflow marks the task as failed, preventing bad data from reaching Power BI.

This is how you achieve 99.5% data integrity: not by hoping the source data is clean, but by asserting specific guarantees at every layer and blocking bad data from advancing.

### Step 8.1 — Create `models/marts/_marts__models.yml`

```yaml
version: 2

models:
  - name: fact_orders
    description: "One row per validated order. Star schema fact table."
    columns:
      - name: order_id
        tests: [not_null, unique]
      - name: customer_id
        tests: [not_null]
      - name: order_date
        tests: [not_null]
      - name: order_amount_usd
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 50000
      - name: order_status
        tests:
          - accepted_values:
              values: ['COMPLETED', 'PENDING', 'REFUNDED', 'CANCELLED']
      - name: acquisition_channel
        tests:
          - accepted_values:
              values: ['organic', 'paid_search', 'email', 'social', 'direct', 'affiliate']

  - name: dim_customers
    description: "Customer dimension with LTV and acquisition data."
    columns:
      - name: customer_id
        tests: [not_null, unique]
      - name: lifetime_value_usd
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 1000000
      - name: customer_segment
        tests:
          - accepted_values:
              values: ['high_value', 'mid_value', 'low_value']

  - name: dim_products
    description: "Product dimension."
    columns:
      - name: product_id
        tests: [not_null, unique]
      - name: product_category
        tests: [not_null]

  - name: fct_daily_metrics
    description: "Pre-aggregated daily KPIs for Power BI."
    columns:
      - name: metric_date
        tests: [not_null]
      - name: cvr_pct
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 100
      - name: bounce_rate_pct
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 100
```

### Step 8.2 — Create `tests/assert_no_future_orders.sql`

This is a **singular test** — a custom SQL query that must return zero rows. If any orders have a date in the future (which indicates a data corruption issue), the test fails.

```sql
-- Returns rows if any orders have a future date — test fails if this returns anything
SELECT
    order_id,
    order_date,
    CURRENT_DATE() AS today
FROM {{ ref('fact_orders') }}
WHERE order_date > CURRENT_DATE()
```

### Step 8.3 — Run all tests

```bash
# Run every test across all models
dbt test

# Run only staging tests
dbt test --select staging

# Run only mart tests
dbt test --select marts

# Expected output for a passing run:
# 14 of 14 passed, 0 warnings, 0 errors

# Generate visual documentation with lineage graph
dbt docs generate
dbt docs serve    # opens http://localhost:8080
```

---

## Phase 9 — Airflow (Astro) Orchestration

### What this phase does

Airflow runs the entire pipeline on a schedule. Without it, you would have to manually trigger data uploads, Snowflake loads, and dbt runs every day. With Airflow, you define the steps once and they run automatically, with retries on failure and alerts when something goes wrong.

### Step 9.1 — Initialise the Astro project

```bash
cd airflow_project
astro dev init
# This creates: dags/, include/, plugins/, Dockerfile, .env, requirements.txt
```

### Step 9.2 — Create `airflow_project/requirements.txt`

```text
apache-airflow-providers-snowflake>=4.0.0
apache-airflow-providers-amazon>=8.0.0
dbt-snowflake>=1.7.0
boto3>=1.26.0
python-dotenv>=1.0.0
```

### Step 9.3 — Create the SQL files for Snowflake loading

**`airflow_project/include/sql/load_orders.sql`:**

```sql
USE DATABASE ECOMMERCE_DB;
USE SCHEMA RAW;
USE WAREHOUSE COMPUTE_WH;

COPY INTO RAW.ORDERS_RAW (
    order_id, customer_id, product_id, order_date, order_status,
    order_amount, currency, channel, discount_pct, shipping_country, _s3_file_path
)
FROM (
    SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, METADATA$FILENAME
    FROM @RAW.S3_RAW_STAGE/orders/date=%(execution_date)s/
)
FILE_FORMAT = (FORMAT_NAME = RAW.CSV_FORMAT)
ON_ERROR    = 'CONTINUE'
PURGE       = FALSE;
```

**`airflow_project/include/sql/load_clicks.sql`:**

```sql
USE DATABASE ECOMMERCE_DB;
USE SCHEMA RAW;
USE WAREHOUSE COMPUTE_WH;

COPY INTO RAW.CLICKS_RAW (
    click_id, session_id, customer_id, product_id,
    event_type, event_timestamp, page_url, referrer_url, device_type, _s3_file_path
)
FROM (
    SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, METADATA$FILENAME
    FROM @RAW.S3_RAW_STAGE/clicks/date=%(execution_date)s/
)
FILE_FORMAT = (FORMAT_NAME = RAW.CSV_FORMAT)
ON_ERROR    = 'CONTINUE'
PURGE       = FALSE;
```

**`airflow_project/include/sql/load_sessions.sql`:**

```sql
USE DATABASE ECOMMERCE_DB;
USE SCHEMA RAW;
USE WAREHOUSE COMPUTE_WH;

COPY INTO RAW.SESSIONS_RAW (
    session_id, customer_id, session_start, session_end, page_views,
    utm_source, utm_medium, utm_campaign, device_type, country, _s3_file_path
)
FROM (
    SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, METADATA$FILENAME
    FROM @RAW.S3_RAW_STAGE/sessions/date=%(execution_date)s/
)
FILE_FORMAT = (FORMAT_NAME = RAW.CSV_FORMAT)
ON_ERROR    = 'CONTINUE'
PURGE       = FALSE;
```

### Step 9.4 — Create `airflow_project/dags/ecommerce_elt_dag.py`

```python
"""
Daily ELT pipeline DAG.
Schedule: 05:00 UTC daily.
Flow: S3 sensors → parallel Snowflake loads → row count validation
      → dbt staging run/test → dbt marts run/test → notify
"""

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "owner":                     "data-engineering",
    "depends_on_past":           False,
    "email":                     ["data-alerts@yourcompany.com"],
    "email_on_failure":          True,
    "email_on_retry":            False,
    "retries":                   3,
    "retry_delay":               timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

S3_BUCKET = os.environ.get("S3_BUCKET_NAME", "your-ecommerce-raw-data")
DBT_DIR   = "/usr/local/airflow/dbt_project"

DBT_ENV = {
    "SNOWFLAKE_ACCOUNT":  os.environ.get("SNOWFLAKE_ACCOUNT", ""),
    "SNOWFLAKE_USER":     os.environ.get("SNOWFLAKE_USER", ""),
    "SNOWFLAKE_PASSWORD": os.environ.get("SNOWFLAKE_PASSWORD", ""),
}

with DAG(
    dag_id            = "ecommerce_elt_daily",
    description       = "Daily ELT: S3 → Snowflake → dbt staging → dbt marts",
    default_args      = default_args,
    start_date        = datetime(2024, 1, 1),
    schedule_interval = "0 5 * * *",
    catchup           = False,
    max_active_runs   = 1,
    tags              = ["ecommerce", "elt", "dbt", "snowflake"],
) as dag:

    # ── Wait for S3 files (runs in parallel) ──────────────────────────────────
    wait_orders = S3KeySensor(
        task_id       = "wait_for_orders_s3",
        bucket_name   = S3_BUCKET,
        bucket_key    = "raw/orders/date={{ ds }}/orders_{{ ds }}.csv",
        aws_conn_id   = "aws_default",
        timeout       = 7200,
        poke_interval = 60,
        mode          = "reschedule",
    )

    wait_clicks = S3KeySensor(
        task_id       = "wait_for_clicks_s3",
        bucket_name   = S3_BUCKET,
        bucket_key    = "raw/clicks/date={{ ds }}/clicks_{{ ds }}.csv",
        aws_conn_id   = "aws_default",
        timeout       = 7200,
        poke_interval = 60,
        mode          = "reschedule",
    )

    wait_sessions = S3KeySensor(
        task_id       = "wait_for_sessions_s3",
        bucket_name   = S3_BUCKET,
        bucket_key    = "raw/sessions/date={{ ds }}/sessions_{{ ds }}.csv",
        aws_conn_id   = "aws_default",
        timeout       = 7200,
        poke_interval = 60,
        mode          = "reschedule",
    )

    # ── Load to Snowflake (runs in parallel after all sensors pass) ───────────
    load_orders = SnowflakeOperator(
        task_id           = "load_orders_to_snowflake",
        snowflake_conn_id = "snowflake_default",
        sql               = "include/sql/load_orders.sql",
        parameters        = {"execution_date": "{{ ds }}"},
    )

    load_clicks = SnowflakeOperator(
        task_id           = "load_clicks_to_snowflake",
        snowflake_conn_id = "snowflake_default",
        sql               = "include/sql/load_clicks.sql",
        parameters        = {"execution_date": "{{ ds }}"},
    )

    load_sessions = SnowflakeOperator(
        task_id           = "load_sessions_to_snowflake",
        snowflake_conn_id = "snowflake_default",
        sql               = "include/sql/load_sessions.sql",
        parameters        = {"execution_date": "{{ ds }}"},
    )

    # ── Validate row counts ───────────────────────────────────────────────────
    validate_counts = SnowflakeOperator(
        task_id           = "validate_row_counts",
        snowflake_conn_id = "snowflake_default",
        sql               = """
            SELECT
                CASE WHEN (SELECT COUNT(*) FROM ECOMMERCE_DB.RAW.ORDERS_RAW
                           WHERE DATE(_loaded_at) = '{{ ds }}') = 0
                     THEN 1/0 ELSE 1 END AS orders_check,
                CASE WHEN (SELECT COUNT(*) FROM ECOMMERCE_DB.RAW.CLICKS_RAW
                           WHERE DATE(_loaded_at) = '{{ ds }}') = 0
                     THEN 1/0 ELSE 1 END AS clicks_check,
                CASE WHEN (SELECT COUNT(*) FROM ECOMMERCE_DB.RAW.SESSIONS_RAW
                           WHERE DATE(_loaded_at) = '{{ ds }}') = 0
                     THEN 1/0 ELSE 1 END AS sessions_check
        """,
    )

    # ── dbt run + test ────────────────────────────────────────────────────────
    dbt_run_staging = BashOperator(
        task_id      = "dbt_run_staging",
        bash_command = f"cd {DBT_DIR} && dbt run --select staging --profiles-dir . --target prod",
        env          = DBT_ENV,
    )

    dbt_test_staging = BashOperator(
        task_id      = "dbt_test_staging",
        bash_command = f"cd {DBT_DIR} && dbt test --select staging --profiles-dir . --target prod",
        env          = DBT_ENV,
    )

    dbt_run_marts = BashOperator(
        task_id      = "dbt_run_marts",
        bash_command = f"cd {DBT_DIR} && dbt run --select marts --profiles-dir . --target prod",
        env          = DBT_ENV,
    )

    dbt_test_marts = BashOperator(
        task_id      = "dbt_test_marts",
        bash_command = f"cd {DBT_DIR} && dbt test --select marts --profiles-dir . --target prod",
        env          = DBT_ENV,
    )

    # ── Notifications ─────────────────────────────────────────────────────────
    notify_success = BashOperator(
        task_id      = "notify_success",
        bash_command = "echo '✅ ELT pipeline completed for {{ ds }}'",
    )

    notify_failure = BashOperator(
        task_id      = "notify_failure",
        bash_command = "echo '❌ ELT pipeline FAILED for {{ ds }}'",
        trigger_rule = TriggerRule.ONE_FAILED,
    )

    # ── DAG wiring ────────────────────────────────────────────────────────────
    [wait_orders, wait_clicks, wait_sessions] >> load_orders
    [wait_orders, wait_clicks, wait_sessions] >> load_clicks
    [wait_orders, wait_clicks, wait_sessions] >> load_sessions

    [load_orders, load_clicks, load_sessions] >> validate_counts

    validate_counts >> dbt_run_staging >> dbt_test_staging
    dbt_test_staging >> dbt_run_marts >> dbt_test_marts
    dbt_test_marts >> notify_success

    [
        wait_orders, wait_clicks, wait_sessions,
        load_orders, load_clicks, load_sessions,
        validate_counts, dbt_run_staging, dbt_test_staging,
        dbt_run_marts, dbt_test_marts,
    ] >> notify_failure
```

### Step 9.5 — Start Airflow and configure connections

```bash
cd airflow_project
astro dev start
# Airflow UI opens at http://localhost:8080
# Default login: admin / admin
```

In the Airflow UI, go to **Admin → Connections → +** and add two connections:

**Snowflake connection:**
- Connection ID: `snowflake_default`
- Connection Type: `Snowflake`
- Host: `your_account.snowflakecomputing.com`
- Schema: `RAW`
- Login: your Snowflake username
- Password: your Snowflake password
- Extra (JSON): `{"account": "your_account", "warehouse": "COMPUTE_WH", "database": "ECOMMERCE_DB", "role": "SYSADMIN"}`

**AWS connection:**
- Connection ID: `aws_default`
- Connection Type: `Amazon Web Services`
- Extra (JSON): `{"aws_access_key_id": "...", "aws_secret_access_key": "..."}`

### Step 9.6 — Enable and trigger the DAG

1. In the Airflow UI, find `ecommerce_elt_daily` in the DAG list
2. Toggle it **ON** (the slider on the left)
3. Click the ▶ (Trigger DAG) button to run it manually for the first time
4. Watch the tasks turn green one by one in the **Graph** view

The DAG will now run automatically every day at 05:00 UTC.

---

## Phase 10 — Power BI Dashboards

### What this phase does

Connects Power BI to Snowflake's MARTS schema and builds the three KPI dashboards: CVR funnel, LTV cohort, and CAC by channel.

### Step 10.1 — Install Power BI Desktop

Download from [powerbi.microsoft.com/downloads](https://powerbi.microsoft.com/downloads) (Windows). Mac users: use Power BI in the browser at [app.powerbi.com](https://app.powerbi.com).

### Step 10.2 — Connect to Snowflake

1. Open Power BI Desktop → **Get Data** → search for **Snowflake** → Connect
2. Fill in:
   - **Server:** `your_account.snowflakecomputing.com`
   - **Warehouse:** `COMPUTE_WH`
3. Click **OK** → sign in with your Snowflake credentials
4. In the Navigator, expand `ECOMMERCE_DB` → `MARTS` → select these four tables:
   - `FACT_ORDERS`
   - `DIM_CUSTOMERS`
   - `DIM_PRODUCTS`
   - `FCT_DAILY_METRICS`
5. Click **Load**

### Step 10.3 — Set up table relationships

In Power BI → **Model view**, create these relationships (if not auto-detected):

| From | To | Join column |
|---|---|---|
| `FACT_ORDERS` | `DIM_CUSTOMERS` | `customer_id` |
| `FACT_ORDERS` | `DIM_PRODUCTS` | `product_id` |
| `FCT_DAILY_METRICS` | `FACT_ORDERS` | `metric_date = order_date` |

### Step 10.4 — Build the dashboards

**Dashboard 1 — CVR Funnel**

Use `FCT_DAILY_METRICS`. Insert a **Funnel chart**:
- Values: `total_sessions`, `add_to_cart_sessions`, `checkout_sessions`, `purchase_sessions`
- Add a **Line chart** for `cvr_pct` over `metric_date`
- Add a **Slicer** on `channel_group` so stakeholders can filter by acquisition channel

**Dashboard 2 — LTV Cohort**

Use `DIM_CUSTOMERS` joined to `FACT_ORDERS`. Insert a **Matrix** visual:
- Rows: `acquisition_cohort_month`
- Columns: months since first order (calculated column)
- Values: cumulative `net_amount_usd`

**Dashboard 3 — CAC by Channel**

Use `FCT_DAILY_METRICS`. Insert a **Bar chart**:
- Axis: `channel_group`
- Values: `new_customers`, `total_revenue_usd`
- Add `cac_usd` once you connect real ad spend data

**Other useful visuals:**
| Visual type | Table | Metric |
|---|---|---|
| KPI card | `FCT_DAILY_METRICS` | Today's `total_revenue_usd` |
| Line chart | `FACT_ORDERS` | Daily `net_amount_usd` over time |
| Bar chart | `DIM_PRODUCTS` joined to `FACT_ORDERS` | Revenue by `product_category` |
| Table | `DIM_CUSTOMERS` | Top customers by `lifetime_value_usd` |

### Step 10.5 — Configure scheduled refresh

1. Click **Publish** in Power BI Desktop → choose your workspace
2. In [app.powerbi.com](https://app.powerbi.com) → go to your dataset → **Settings**
3. Under **Scheduled refresh** → enable it → set frequency to daily (or every 2 hours)
4. Configure the Snowflake data source credentials under **Data source credentials**

---

## Phase 11 — Running the Full Pipeline

### Step 11.1 — First-time end-to-end run

Follow these steps in order:

```bash
# ── Step 1: Backfill 30 days of synthetic data to S3 ─────────────────────────
cd data_generator
python upload_to_s3.py --start-date 2024-01-01 --end-date 2024-01-30

# ── Step 2: Run Snowflake setup SQL (if not done yet) ────────────────────────
# In Snowflake Worksheet, run all three files in snowflake_setup/ in order

# ── Step 3: Run dbt manually to verify it works ──────────────────────────────
cd ../dbt_project
dbt run
dbt test
# All tests should pass

# ── Step 4: Start Airflow ─────────────────────────────────────────────────────
cd ../airflow_project
astro dev start
# Go to http://localhost:8080 → enable ecommerce_elt_daily → trigger manually

# ── Step 5: Verify Snowflake has data ────────────────────────────────────────
# In Snowflake Worksheet:
SELECT COUNT(*) FROM ECOMMERCE_DB.MARTS.FACT_ORDERS;
SELECT COUNT(*) FROM ECOMMERCE_DB.MARTS.FCT_DAILY_METRICS;
SELECT * FROM ECOMMERCE_DB.MARTS.FCT_DAILY_METRICS LIMIT 5;

# ── Step 6: Refresh Power BI ─────────────────────────────────────────────────
# In Power BI Desktop: Home → Refresh
```

### Step 11.2 — Daily automated flow (once set up)

Every day at 05:00 UTC, Airflow runs automatically:

```
05:00  S3 sensors activate — wait for upload scripts to deliver today's files
05:xx  Files detected — parallel COPY INTO loads start (orders, clicks, sessions)
05:xx  Row count validation — fails if any table loaded 0 rows
05:xx  dbt staging run → dbt staging tests
05:xx  dbt marts run → dbt marts tests
05:xx  Success notification sent
```

Total runtime is typically 15–30 minutes, giving Power BI data for the previous day by ~05:30 UTC — well within the 2-hour latency target.

### Step 11.3 — Monitoring checklist

Check these daily (or set up alerts so you only check when something fails):

```bash
# Airflow — all runs green?
# http://localhost:8080 → DAGs → ecommerce_elt_daily → recent runs all ✅

# dbt — all tests passing?
cd dbt_project && dbt test
# Expected: "X of X passed, 0 failed"

# Snowflake — today's data loaded?
# In Snowflake Worksheet:
SELECT COUNT(*) FROM ECOMMERCE_DB.MARTS.FACT_ORDERS
WHERE order_date = CURRENT_DATE() - 1;

# S3 — yesterday's files present?
aws s3 ls s3://your-bucket/raw/orders/ --recursive | grep $(date -d "yesterday" +%Y-%m-%d)
```

---

## Troubleshooting

### `dbt debug` fails — Snowflake connection error

- Your `SNOWFLAKE_ACCOUNT` in `.env` must NOT include `.snowflakecomputing.com` — just the locator, e.g. `abc12345.us-east-1`
- Verify your password doesn't contain special characters that need escaping in YAML
- Check your Snowflake trial hasn't expired (30-day limit)
- Run `dbt debug --profiles-dir .` to confirm dbt is reading the right profiles file

### S3 access denied

```bash
# Check your credentials are loaded
aws sts get-caller-identity
# If this fails, re-run: aws configure

# Check your IAM user has S3 permissions
aws s3 ls s3://your-bucket-name/
# Access denied = IAM policy issue, not credential issue
```

### Airflow can't find the DAG

```bash
# Test for Python syntax errors:
python airflow_project/dags/ecommerce_elt_dag.py
# No output = no errors

# Restart Airflow to pick up the new file:
astro dev restart
```

### dbt COPY INTO fails in Airflow

- Confirm `S3_RAW_STAGE` exists: run `SHOW STAGES IN SCHEMA ECOMMERCE_DB.RAW;` in Snowflake
- Confirm the S3 path in the stage matches where your files actually are: run `LIST @RAW.S3_RAW_STAGE;`
- Check `ON_ERROR = 'CONTINUE'` is set — this logs bad rows instead of failing the whole load

### Power BI can't connect to Snowflake

- Install the [Snowflake ODBC driver](https://docs.snowflake.com/en/user-guide/odbc.html)
- Check that your Snowflake warehouse isn't suspended. In Snowflake Worksheet: `ALTER WAREHOUSE COMPUTE_WH RESUME;`
- Ensure you're using the correct account identifier format in Power BI (same as in `profiles.yml`)

### Snowflake is running out of trial credits faster than expected

- Set `AUTO_SUSPEND = 60` on your warehouse (should already be set from Phase 3)
- Reduce the size of your synthetic data: edit `upload_to_s3.py` and lower the row counts
- Check what's consuming credits: in Snowflake → **Admin → Cost Management → Warehouse Usage**

---

*End of course. You now have a fully working, production-style ELT pipeline.*

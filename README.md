# End-to-End ELT Pipeline — E-Commerce Analytics
### Stack: AWS S3 · Snowflake · dbt · Apache Airflow (Astronomer/Astro) · Power BI

---

## Table of Contents
1. [Project Overview](#1-project-overview)
2. [Architecture](#2-architecture)
3. [Prerequisites & Accounts](#3-prerequisites--accounts)
4. [Phase 1 — Project Structure Setup](#phase-1--project-structure-setup)
5. [Phase 2 — AWS S3 Setup (Raw Landing Zone)](#phase-2--aws-s3-setup-raw-landing-zone)
6. [Phase 3 — Snowflake Setup](#phase-3--snowflake-setup)
7. [Phase 4 — Data Generation (Synthetic E-Commerce Data)](#phase-4--data-generation-synthetic-e-commerce-data)
8. [Phase 5 — dbt Project Setup](#phase-5--dbt-project-setup)
9. [Phase 6 — dbt Models (Staging → Marts)](#phase-6--dbt-models-staging--marts)
10. [Phase 7 — dbt Tests & Data Quality](#phase-7--dbt-tests--data-quality)
11. [Phase 8 — Airflow (Astro) Setup & DAG](#phase-8--airflow-astro-setup--dag)
12. [Phase 9 — Power BI Dashboard Setup](#phase-9--power-bi-dashboard-setup)
13. [Phase 10 — Running the Full Pipeline End-to-End](#phase-10--running-the-full-pipeline-end-to-end)
14. [Troubleshooting](#troubleshooting)

---

## 1. Project Overview

This project implements a **production-grade ELT pipeline** for e-commerce analytics. Raw event data (orders, clicks, sessions) lands in AWS S3, gets loaded into Snowflake, transformed by dbt into a star schema, orchestrated daily by Airflow, and visualised in Power BI.

**Key metrics tracked:** Conversion Rate (CVR), Customer Lifetime Value (LTV), Customer Acquisition Cost (CAC).

**Daily volume:** 500,000+ records with automated schema validation, deduplication, and multi-level quality checks targeting 99.5% data integrity.

---

## 2. Architecture

```
[E-Commerce Sources]
   Orders / Clicks / Sessions (CSV or JSON)
          │
          ▼ (Python upload script / daily batch)
   [AWS S3 — Raw Landing Zone]
   s3://your-bucket/raw/orders/date=YYYY-MM-DD/
   s3://your-bucket/raw/clicks/date=YYYY-MM-DD/
   s3://your-bucket/raw/sessions/date=YYYY-MM-DD/
          │
          ▼ (Airflow triggers COPY INTO)
   [Snowflake — RAW schema]
   raw.orders_raw, raw.clicks_raw, raw.sessions_raw
          │
          ▼ (dbt run — staging models)
   [Snowflake — STAGING schema]
   staging.stg_orders, staging.stg_clicks, staging.stg_sessions
          │
          ▼ (dbt run — mart models)
   [Snowflake — MARTS schema]
   marts.fact_orders, marts.dim_customers, marts.dim_products
   marts.fct_daily_metrics (CVR, LTV, CAC aggregates)
          │
          ▼ (DirectQuery or scheduled refresh)
   [Power BI Dashboard]
   CVR funnel · LTV cohort · CAC by channel
```

**Airflow (Astro) orchestrates the entire flow** from S3 ingestion through Snowflake loading, dbt transformations, and quality checks — on a daily schedule with automatic retries and Slack/email alerting on failure.

---

## 3. Prerequisites & Accounts

You need free/trial accounts for:

| Service | Free Tier | URL |
|---|---|---|
| AWS | 12-month free tier (S3 free up to 5 GB) | https://aws.amazon.com/free |
| Snowflake | 30-day $400 trial | https://signup.snowflake.com |
| Astronomer (Airflow) | Free local dev with Astro CLI | https://www.astronomer.io/try-astro |
| Power BI Desktop | Free download | https://powerbi.microsoft.com/downloads |
| dbt Core | Free, installed via pip | https://docs.getdbt.com |

**Local tools to install:**
```bash
# Python 3.9+
python --version

# AWS CLI
pip install awscli
aws configure   # enter your AWS Access Key ID and Secret

# Astro CLI (Airflow)
# Mac:
brew install astro
# Windows (PowerShell):
winget install -e --id Astronomer.Astro

# dbt with Snowflake adapter
pip install dbt-snowflake
```

---

## Phase 1 — Project Structure Setup

### 1.1 Create the project folder

```bash
mkdir ecommerce_elt_pipeline
cd ecommerce_elt_pipeline
```

### 1.2 Final folder structure (build this incrementally across phases)

```
ecommerce_elt_pipeline/
├── README.md
├── .env                          # secrets — never commit this!
├── .gitignore
│
├── data_generator/               # Phase 4: synthetic data scripts
│   ├── generate_orders.py
│   ├── generate_clicks.py
│   ├── generate_sessions.py
│   └── upload_to_s3.py
│
├── snowflake_setup/              # Phase 3: SQL setup scripts
│   ├── 01_account_setup.sql
│   ├── 02_schemas_tables.sql
│   └── 03_stages.sql
│
├── dbt_project/                  # Phase 5-7: dbt
│   ├── dbt_project.yml
│   ├── profiles.yml
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
└── airflow_project/              # Phase 8: Airflow (Astro)
    ├── dags/
    │   └── ecommerce_elt_dag.py
    ├── include/
    │   └── sql/
    │       ├── load_orders.sql
    │       ├── load_clicks.sql
    │       └── load_sessions.sql
    ├── requirements.txt
    └── Dockerfile
```

### 1.3 Create .gitignore

```gitignore
# Secrets
.env
profiles.yml
*.key

# Python
__pycache__/
*.pyc
.venv/
venv/

# dbt
dbt_project/target/
dbt_project/logs/
dbt_project/.user.yml

# Airflow
airflow_project/logs/
airflow_project/.astro/
```

### 1.4 Create .env file (fill in your values)

```env
# AWS
AWS_ACCESS_KEY_ID=your_key_here
AWS_SECRET_ACCESS_KEY=your_secret_here
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET_NAME=your-ecommerce-raw-data

# Snowflake
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=ECOMMERCE_DB
SNOWFLAKE_ROLE=SYSADMIN
```

> **How to find your Snowflake account identifier:** In Snowflake, go to Admin → Accounts. It looks like `abc12345.us-east-1`. Use that full string (without `.snowflakecomputing.com`).

---

## Phase 2 — AWS S3 Setup (Raw Landing Zone)

### 2.1 Create an S3 bucket

In AWS Console → S3 → Create bucket:
- **Bucket name:** `your-ecommerce-raw-data` (must be globally unique — add your initials)
- **Region:** `us-east-1` (or your preferred region)
- **Block all public access:** ✅ Yes (keep it private)
- All other defaults → Create

### 2.2 Create IAM user for programmatic access

In AWS Console → IAM → Users → Create user:
- **Username:** `ecommerce-pipeline-user`
- **Permission policies:** Attach `AmazonS3FullAccess` (or create a scoped policy — see below)
- Download the **Access Key ID** and **Secret Access Key** → add them to your `.env`

**Scoped IAM policy (recommended for production):**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::your-ecommerce-raw-data",
        "arn:aws:s3:::your-ecommerce-raw-data/*"
      ]
    }
  ]
}
```

### 2.3 Configure AWS CLI

```bash
aws configure
# AWS Access Key ID: (paste from .env)
# AWS Secret Access Key: (paste from .env)
# Default region name: us-east-1
# Default output format: json

# Test it works:
aws s3 ls
```

### 2.4 S3 folder structure

The pipeline uses date-partitioned folders. You don't need to create them manually — the upload script handles it. The structure will be:
```
s3://your-ecommerce-raw-data/
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

---

## Phase 3 — Snowflake Setup

### 3.1 Log into Snowflake

Go to https://app.snowflake.com → sign in with your trial credentials.

### 3.2 Run the setup SQL scripts in order

Open a **Snowflake Worksheet** and run each script in `snowflake_setup/`. The scripts are provided in the `snowflake_setup/` folder of this project. Run them in this order:

1. `01_account_setup.sql` — creates database, warehouse, roles
2. `02_schemas_tables.sql` — creates RAW, STAGING, MARTS schemas and raw tables
3. `03_stages.sql` — creates external S3 stage for COPY INTO

### 3.3 S3 Stage: connecting Snowflake to S3

The stage in `03_stages.sql` tells Snowflake where to find your S3 data. After running the script, you'll have:
```sql
-- Snowflake can now read from your S3 bucket directly
SHOW STAGES IN SCHEMA RAW;
-- You should see: S3_RAW_STAGE
```

---

## Phase 4 — Data Generation (Synthetic E-Commerce Data)

All scripts are in `data_generator/`. They generate realistic CSV data and upload to S3.

### 4.1 Install dependencies

```bash
pip install boto3 faker pandas python-dotenv
```

### 4.2 Generate and upload data

```bash
cd data_generator

# Generate one day of data (default: today's date)
python generate_orders.py
python generate_clicks.py
python generate_sessions.py

# Upload everything to S3
python upload_to_s3.py

# Or generate + upload for a date range (for backfilling):
python upload_to_s3.py --start-date 2024-01-01 --end-date 2024-01-30
```

### 4.3 Verify data in S3

```bash
aws s3 ls s3://your-ecommerce-raw-data/raw/orders/ --recursive
```

---

## Phase 5 — dbt Project Setup

### 5.1 Navigate to dbt project folder

```bash
cd dbt_project
```

### 5.2 Install dbt Snowflake adapter

```bash
pip install dbt-snowflake
dbt --version   # should show dbt-core and dbt-snowflake versions
```

### 5.3 Configure `profiles.yml`

`profiles.yml` stores your Snowflake connection. **Do NOT commit this file** (it's in `.gitignore`).

Create it at `~/.dbt/profiles.yml` (dbt's default location) OR in your project root. The file is provided in this project — fill in your credentials from `.env`.

### 5.4 Install dbt packages

```bash
dbt deps
```

This installs `dbt_utils` and `dbt_expectations` from `packages.yml`.

### 5.5 Test connection

```bash
dbt debug
# Should show: All checks passed!
```

### 5.6 Run models for the first time

```bash
# Run all models
dbt run

# Run only staging
dbt run --select staging

# Run only marts
dbt run --select marts

# Run tests
dbt test

# Generate and view docs
dbt docs generate
dbt docs serve   # opens browser at http://localhost:8080
```

---

## Phase 6 — dbt Models (Staging → Marts)

All model files are in `dbt_project/models/`. Here's what each layer does:

### Staging layer (`models/staging/`)
- **`stg_orders.sql`** — cleans raw orders: casts types, renames columns, adds `loaded_at` timestamp, deduplicates by `order_id`
- **`stg_clicks.sql`** — cleans raw click events: parses timestamps, validates event types
- **`stg_sessions.sql`** — cleans sessions: calculates session duration, flags bounced sessions

### Marts layer (`models/marts/`)
- **`fact_orders.sql`** — grain: one row per order. Joins to dim tables, adds revenue metrics
- **`dim_customers.sql`** — SCD Type 1 customer dimension with acquisition channel, first/last order dates
- **`dim_products.sql`** — product catalog dimension
- **`fct_daily_metrics.sql`** — pre-aggregated: daily CVR, LTV cohorts, CAC by channel — feeds directly into Power BI

### Star schema diagram
```
              dim_customers
                    │
                    │ customer_id
                    │
dim_products ─── fact_orders ─── dim_date (generated by dbt)
                    │
                    └── fct_daily_metrics (aggregated view)
```

---

## Phase 7 — dbt Tests & Data Quality

### 7.1 Built-in dbt tests (in `_marts__models.yml`)
- `not_null` on all primary keys and critical columns
- `unique` on all primary keys
- `accepted_values` on status and channel columns
- `relationships` (referential integrity between fact and dim tables)

### 7.2 dbt_expectations tests
- `expect_column_values_to_be_between` — order amounts between $0 and $50,000
- `expect_column_proportion_of_unique_values_to_be_between` — dedup rate check
- `expect_table_row_count_to_be_between` — daily volume sanity check (min 100 rows)

### 7.3 Custom singular test
- `tests/assert_no_future_orders.sql` — no orders with `order_date > current_date`

### 7.4 Run tests

```bash
dbt test                          # run all tests
dbt test --select staging         # only staging tests
dbt test --select tag:critical    # only tests tagged 'critical'
```

---

## Phase 8 — Airflow (Astro) Setup & DAG

### 8.1 Initialize Astro project

```bash
cd airflow_project
astro dev init     # creates the Astro project structure
```

### 8.2 Start local Airflow

```bash
astro dev start
# Airflow UI: http://localhost:8080
# Username: admin  Password: admin
```

### 8.3 Add connections in Airflow UI

Go to Admin → Connections → Add:

**Snowflake connection:**
- Connection ID: `snowflake_default`
- Connection Type: `Snowflake`
- Host: `your_account.snowflakecomputing.com`
- Schema: `RAW`
- Login: your Snowflake username
- Password: your Snowflake password
- Extra: `{"account": "your_account", "warehouse": "COMPUTE_WH", "database": "ECOMMERCE_DB", "role": "SYSADMIN"}`

**AWS connection:**
- Connection ID: `aws_default`
- Connection Type: `Amazon Web Services`
- Extra: `{"aws_access_key_id": "...", "aws_secret_access_key": "..."}`

### 8.4 Add the DAG

Copy `dags/ecommerce_elt_dag.py` into `airflow_project/dags/`. It will auto-appear in the Airflow UI.

### 8.5 DAG structure

```
[S3FileSensor]
     │  waits for today's files to land in S3
     ▼
[load_orders_to_snowflake] ─┐
[load_clicks_to_snowflake]  ├─ parallel
[load_sessions_to_snowflake]┘
     │
     ▼
[validate_row_counts]
     │
     ▼
[dbt_run_staging]
     │
     ▼
[dbt_test_staging]
     │
     ▼
[dbt_run_marts]
     │
     ▼
[dbt_test_marts]
     │
     ▼
[notify_success]   (or [notify_failure] on any task error)
```

### 8.6 Schedule

The DAG runs daily at **05:00 UTC** (`schedule_interval="0 5 * * *"`). Data latency from source to Power BI is under 2 hours.

---

## Phase 9 — Power BI Dashboard Setup

### 9.1 Install Power BI Desktop

Download from https://powerbi.microsoft.com/downloads (free, Windows only; Mac users use Power BI web via browser).

### 9.2 Connect to Snowflake

1. Open Power BI Desktop → Get Data → Snowflake
2. Server: `your_account.snowflakecomputing.com`
3. Warehouse: `COMPUTE_WH`
4. Database: `ECOMMERCE_DB`
5. Schema: `MARTS`
6. Select tables: `FACT_ORDERS`, `DIM_CUSTOMERS`, `DIM_PRODUCTS`, `FCT_DAILY_METRICS`

### 9.3 Configure scheduled refresh (Power BI Service)

1. Publish report to Power BI Service (free with Microsoft account)
2. Dataset Settings → Scheduled Refresh → Daily (every 2 hours or on a fixed schedule)
3. Ensure the Snowflake gateway connection is configured

### 9.4 Key visuals to build

| Visual | Table | Columns |
|---|---|---|
| CVR Funnel | `fct_daily_metrics` | `sessions_count`, `clicks_count`, `orders_count` |
| LTV by Cohort | `dim_customers` + `fact_orders` | `acquisition_month`, `cumulative_revenue` |
| CAC by Channel | `fct_daily_metrics` | `channel`, `ad_spend`, `new_customers` |
| Daily Revenue | `fact_orders` | `order_date`, `order_amount` |
| Top Products | `dim_products` + `fact_orders` | `product_name`, `total_revenue` |

---

## Phase 10 — Running the Full Pipeline End-to-End

### 10.1 First-time full run

```bash
# 1. Generate synthetic data for the past 30 days
cd data_generator
python upload_to_s3.py --start-date 2024-01-01 --end-date 2024-01-30

# 2. Run Snowflake setup (do this once in Snowflake Worksheet)
# Run snowflake_setup/01_account_setup.sql
# Run snowflake_setup/02_schemas_tables.sql
# Run snowflake_setup/03_stages.sql

# 3. Load raw data manually (or trigger Airflow)
# In Snowflake Worksheet, run the COPY INTO commands from snowflake_setup/02_schemas_tables.sql

# 4. Run dbt
cd dbt_project
dbt run
dbt test

# 5. Start Airflow
cd airflow_project
astro dev start
# Enable the DAG in the Airflow UI → ecommerce_elt_daily
# Trigger manually for a backfill
```

### 10.2 Daily automated flow (once set up)

Airflow runs at 05:00 UTC automatically:
1. Waits for S3 files
2. Loads to Snowflake RAW
3. Runs dbt staging + tests
4. Runs dbt marts + tests
5. Sends success/failure notification

### 10.3 Verify everything works

```bash
# Check Snowflake has data
# In Snowflake Worksheet:
SELECT COUNT(*) FROM ECOMMERCE_DB.MARTS.FACT_ORDERS;
SELECT COUNT(*) FROM ECOMMERCE_DB.MARTS.FCT_DAILY_METRICS;

# Check dbt tests all pass
cd dbt_project && dbt test

# Check Airflow DAG runs are green
# http://localhost:8080 → DAGs → ecommerce_elt_daily → all green checkmarks
```

---

## Troubleshooting

### "dbt debug" fails — Snowflake connection error
- Double-check your account identifier (no `.snowflakecomputing.com` suffix in `profiles.yml`)
- Verify password has no special chars that need escaping
- Check your Snowflake trial hasn't expired

### S3 access denied
- Run `aws sts get-caller-identity` — if it fails, re-run `aws configure`
- Make sure your IAM user has S3 permissions

### Airflow can't find the DAG
- Check the file is in `airflow_project/dags/` with no Python syntax errors: `python dags/ecommerce_elt_dag.py`
- Run `astro dev restart` to pick up changes

### dbt COPY INTO fails
- Ensure the Snowflake stage (`S3_RAW_STAGE`) is created correctly
- Verify the S3 bucket path matches the stage definition
- Check the file format (CSV vs JSON) matches the `FILE FORMAT` in the stage

### Power BI can't connect to Snowflake
- Install the Snowflake ODBC driver
- Check your Snowflake warehouse is not suspended (run `ALTER WAREHOUSE COMPUTE_WH RESUME;`)

import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px
from dotenv import load_dotenv
import os

load_dotenv()

# ── Connection ──────────────────────────────────────────
@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema="MARTS"
    )

@st.cache_data(ttl=3600)
def run_query(query):
    conn = get_connection()
    return pd.read_sql(query, conn)

# ── Page config ─────────────────────────────────────────
st.set_page_config(page_title="E-Commerce KPIs", layout="wide")
st.title("📊 E-Commerce KPI Dashboard")

# ── Load data ───────────────────────────────────────────
metrics   = run_query("SELECT * FROM FCT_DAILY_METRICS ORDER BY METRIC_DATE")
orders    = run_query("SELECT * FROM FACT_ORDERS")
customers = run_query("SELECT * FROM DIM_CUSTOMERS")

# ── Sidebar filter ──────────────────────────────────────
channels = ["All"] + list(metrics["CHANNEL_GROUP"].dropna().unique())
selected_channel = st.sidebar.selectbox("Filter by Channel", channels)

if selected_channel != "All":
    metrics = metrics[metrics["CHANNEL_GROUP"] == selected_channel]

# ── KPI Cards ───────────────────────────────────────────
st.subheader("Today's KPIs")
col1, col2, col3 = st.columns(3)
latest = metrics.iloc[-1]
col1.metric("Total Revenue", f"${latest['TOTAL_REVENUE_USD']:,.0f}")
col2.metric("CVR %", f"{latest['CVR_PCT']:.2f}%")
col3.metric("CAC", f"${latest['CAC_USD']:,.2f}" if 'CAC_USD' in latest else "N/A")

st.divider()

# ── Dashboard 1: CVR Funnel ─────────────────────────────
st.subheader("Conversion Rate Funnel")

funnel_data = pd.DataFrame({
    "Stage": ["Sessions", "Add to Cart", "Checkout", "Purchase"],
    "Count": [
        metrics["TOTAL_SESSIONS"].sum(),
        metrics["ADD_TO_CART_SESSIONS"].sum(),
        metrics["CHECKOUT_SESSIONS"].sum(),
        metrics["PURCHASE_SESSIONS"].sum()
    ]
})
fig1 = px.funnel(funnel_data, x="Count", y="Stage")
st.plotly_chart(fig1, use_container_width=True)

st.line_chart(metrics.set_index("METRIC_DATE")["CVR_PCT"])

st.divider()

# ── Dashboard 2: LTV Cohort ─────────────────────────────
st.subheader("LTV Cohort Analysis")

cohort_df = customers.merge(orders, on="CUSTOMER_ID")
cohort_df["MONTHS_SINCE_FIRST"] = (
    pd.to_datetime(cohort_df["ORDER_DATE"]).dt.to_period("M").astype(int) -
    pd.to_datetime(cohort_df["ACQUISITION_COHORT_MONTH"]).dt.to_period("M").astype(int)
)
cohort_pivot = cohort_df.groupby(
    ["ACQUISITION_COHORT_MONTH", "MONTHS_SINCE_FIRST"]
)["NET_AMOUNT_USD"].sum().unstack()

st.dataframe(cohort_pivot.style.background_gradient(cmap="Blues"), use_container_width=True)

st.divider()

# ── Dashboard 3: CAC by Channel ─────────────────────────
st.subheader("CAC by Channel")

cac_df = metrics.groupby("CHANNEL_GROUP").agg(
    New_Customers=("NEW_CUSTOMERS", "sum"),
    Total_Revenue=("TOTAL_REVENUE_USD", "sum")
).reset_index()

fig3 = px.bar(cac_df, x="CHANNEL_GROUP", y=["New_Customers", "Total_Revenue"],
              barmode="group", title="New Customers & Revenue by Channel")
st.plotly_chart(fig3, use_container_width=True)

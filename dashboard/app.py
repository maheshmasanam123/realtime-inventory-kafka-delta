"""Streamlit dashboard reading live stock state from Delta Lake on MinIO.

Auto-refreshes every 5 seconds.
"""
import time

import duckdb
import pandas as pd
import streamlit as st


REFRESH_SECONDS = 5
DELTA_PATH = "s3a://inventory/state"


def load_state() -> pd.DataFrame:
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET s3_endpoint='localhost:9000'; SET s3_use_ssl=false; SET s3_url_style='path';")
    con.execute("SET s3_access_key_id='admin'; SET s3_secret_access_key='admin12345';")
    con.execute("INSTALL delta; LOAD delta;")
    return con.execute(f"SELECT * FROM delta_scan('{DELTA_PATH}')").df()


st.set_page_config(page_title="Live Inventory", layout="wide")
st.title("Live Inventory Stock Levels")

placeholder = st.empty()
while True:
    try:
        df = load_state()
    except Exception as exc:
        st.warning(f"waiting for first batch: {exc}")
        time.sleep(REFRESH_SECONDS)
        continue

    with placeholder.container():
        c1, c2, c3 = st.columns(3)
        c1.metric("SKUs tracked", f"{df['sku'].nunique():,}")
        c2.metric("Warehouses",   f"{df['warehouse_id'].nunique():,}")
        c3.metric("Total on hand", f"{int(df['on_hand'].sum()):,}")

        st.subheader("Top 20 SKUs by on-hand")
        st.dataframe(df.sort_values("on_hand", ascending=False).head(20), use_container_width=True)

        st.subheader("Negative-stock alerts")
        neg = df[df["on_hand"] < 0]
        if len(neg):
            st.error(f"{len(neg)} (warehouse, sku) pairs are negative — investigate")
            st.dataframe(neg, use_container_width=True)
        else:
            st.success("No negative stock detected")

    time.sleep(REFRESH_SECONDS)

import streamlit as st
from pymongo import MongoClient
import pandas as pd
import os

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.getenv("MONGO_DB", "analytics")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "aggregates")

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
coll = db[MONGO_COLLECTION]

st.title("Streaming analytics dashboard")

data = list(coll.find().sort([("_id", -1)]).limit(200))
if not data:
    st.info("No aggregated data yet")
else:
    df = pd.DataFrame(data)
    # drop mongo _id for display
    if "_id" in df.columns:
        df = df.drop(columns=["_id"])
    st.dataframe(df)
    # simple chart: avg_sales by Order Region
    if "Order Region" in df.columns and "avg_sales" in df.columns:
        chart_df = df.groupby("Order Region").agg({"avg_sales":"mean"}).reset_index()
        st.bar_chart(chart_df.rename(columns={"Order Region":"index"}).set_index("index"))

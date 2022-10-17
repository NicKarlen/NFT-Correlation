"""
    to run the script enter in Terminal:

    streamlit run c:/Git/NFT-Correlation/src/web_show.py
"""
import streamlit as st
import functions as f


x = st.slider('Select a value', min_value=5,max_value=50)

df = f.read_df_from_sql(table_name="degods")
df["PercChanges"] = df["cFP in Dollar"].pct_change() * 100
df["MA"] = df["PercChanges"].rolling(window=x).mean()

st.dataframe(df[["ts","cFP in Dollar", "PercChanges", "MA"]])

st.line_chart(data=df, x="ts",y="MA")
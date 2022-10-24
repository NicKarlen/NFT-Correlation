"""
    to run the script enter in Terminal:

    docs:
    https://docs.streamlit.io/library/api-reference/utilities/st.set_page_config

    how to deploy:
    https://towardsdatascience.com/3-easy-ways-to-deploy-your-streamlit-web-app-online-7c88bb1024b1

    command to run the server:
    streamlit run c:/Git/NFT-Correlation/src/web_show.py
"""
from time import sleep
import streamlit as st
import functions as f
import main as main
from datetime import datetime
import pandas as pd

st.set_page_config(page_title="AFE2 Correlation", page_icon=None, layout="wide", initial_sidebar_state="auto", menu_items=None)

# Navbar (Sidebar)
with st.sidebar:
    st.header("Control Panel")
    # step_1
    collection = st.selectbox(label='Solana NFT Collection', options=main.ALL_COLLECTIONS)
    tradingpairs = st.multiselect(label='Traidingpairs', options=main.ALL_TRADINGPAIRS, default=["SOLUSDT"])
    if st.button("Collect & Prepare fresh data"):
        print([collection])
        with st.spinner('Wait for it...'):
            main.step_1(webvisu=True, input_collections=[collection])
            main.step_2(webvisu=True, input_tradingpairs=tradingpairs)
            main.step_3(webvisu=True, input_collections=[collection])
        st.success('Done!')
 


st.header("Correlation between NFT's and Crypto-Assets")
st.subheader(f"Selected Asset: {collection}")

x = st.slider('Moving average of:', min_value=5,max_value=50)

try:
    df = f.read_df_from_sql(table_name=collection)

    df["MA"] = df["cFP in Dollar"].rolling(window=x).mean()
    df["ts"] = df["ts"] / 1000
    df["timestamp"] = df["ts"].apply(datetime.fromtimestamp)

    st.line_chart(data=df, x="timestamp",y="MA")
    #st.dataframe(df[["ts","timestamp","cFP in Dollar", "MA"]])
except:
    st.warning("Data not ready yet...")


st.subheader(f"Correlation Matrix")
try:
    print(tradingpairs, [collection])
    df_merged = f.create_single_table(tradingpairs=tradingpairs, collections=[collection])
    f.write_df_to_sql(df=df_merged, table_name="df_merge")
    df_corr = f.calc_pearson_coefficient_matrix(tradingpairs=tradingpairs, collections=[collection])
    st.table(df_corr)
except:
    st.warning("Data not ready yet...")
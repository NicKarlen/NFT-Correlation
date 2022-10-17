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

st.set_page_config(page_title="AFE2 Correlation", page_icon=None, layout="wide", initial_sidebar_state="auto", menu_items=None)

# Navbar (Sidebar)
with st.sidebar:
    st.header("Control Panel")
    # step_1
    collections = st.text_input(label='Solana NFT Collection', value='degods') #, help='more than 2 separate with ","')
    tradingpairs = st.text_input(label='Traidingpairs', value='BTCUSDT')#, help='more than 2 separate with ","'
    if st.button("1: Collect data from MagicEden"):
        print(collections.split(","))
        with st.spinner('Wait for it...'):
            main.step_1(webvisu=True, input_collections=collections.split(","))
        st.success('Done!')

    # step_2
    
    if st.button("2: Collect data from Binance"):
        print(tradingpairs.split(","))
        with st.spinner('Wait for it...'):
            main.step_2(webvisu=True, input_tradingpairs=tradingpairs.split(","))
        st.success('Done!')

    # step_3
    if st.button("3: Calc $-value for collections"):
        with st.spinner('Wait for it...'):
            main.step_3(webvisu=True, input_collections=collections.split(","))
        st.success('Done!')   


st.header("Correlation between NFT's and Crypto-Assets")
st.write("Selected Asset: ", collections)

x = st.slider('Moving average of:', min_value=5,max_value=50)

try:
    df = f.read_df_from_sql(table_name="degods")

    df["MA"] = df["cFP in Dollar"].rolling(window=x).mean()

    st.line_chart(data=df, x="ts",y="MA")
    st.dataframe(df[["ts","cFP in Dollar", "MA"]])
   
except:
    st.warning("Data not ready yet...")
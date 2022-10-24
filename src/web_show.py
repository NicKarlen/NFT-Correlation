"""
    to run the script enter in Terminal:

    docs:
    https://docs.streamlit.io/library/api-reference/utilities/st.set_page_config

    how to deploy:
    https://towardsdatascience.com/3-easy-ways-to-deploy-your-streamlit-web-app-online-7c88bb1024b1

    command to run the server:
    streamlit run c:/Git/NFT-Correlation/src/web_show.py
"""
import streamlit as st
import functions as f
import main as main
from datetime import datetime
import pandas as pd

st.set_page_config(page_title="AFE2 Correlation", page_icon=None, initial_sidebar_state="auto", menu_items=None) #, layout="wide"


df_top_collections = f.read_df_from_sql(table_name="Solana_Collections")
df_top_collections.sort_values(by="totalVol", ascending=False,inplace=True)
arr_top_collection = df_top_collections["collectionSymbol"].values

# Navbar (Sidebar)
with st.sidebar:
    st.header("Control Panel")
    # step_1
    collection = st.selectbox(label='Solana NFT Collection', options=arr_top_collection)
    tradingpairs = st.multiselect(label='Traidingpairs', options=main.ALL_TRADINGPAIRS, default=["SOLUSDT"])
    if st.button("Collect & Prepare fresh data"):
        with st.spinner('Wait for it...'):
            main.step_1(webvisu=True, input_collections=[collection])
            main.step_2(webvisu=True, input_tradingpairs=tradingpairs)
            main.step_3(webvisu=True, input_collections=[collection])
        st.success('Done!')
 


st.header("Correlation between NFT's and Crypto-Assets")
st.subheader(f"Selected Asset: {collection}")
st.write(f"Shows the moving avg. of the dollar value of {collection}")

x = st.slider('Moving average of:', min_value=5,max_value=50)

try:
    df = f.read_df_from_sql(table_name=collection)

    df["USD"] = df["cFP in Dollar"].rolling(window=x).mean() # MA 
    df["ts"] = df["ts"] / 1000
    df["timestamp"] = df["ts"].apply(datetime.fromtimestamp)

    st.line_chart(data=df, x="timestamp",y="USD")
    #st.dataframe(df[["ts","timestamp","cFP in Dollar", "MA"]])
except:
    st.warning("Data not ready yet...")


st.subheader(f"Correlation Matrix")
try:
    df_merged = f.create_single_table(tradingpairs=tradingpairs, collections=[collection])
    f.write_df_to_sql(df=df_merged, table_name="df_merge")
    df_corr = f.calc_pearson_coefficient_matrix(tradingpairs=tradingpairs, collections=[collection])
    st.table(df_corr)
except:
    st.warning("Data not ready yet...")


try:

    st.subheader(f"Correlation graph of {tradingpairs[0]} <-> {collection}")
    st.write("Only takes the first Tradingpair in the selection and compares it to the NFT-Collection")
    w_length = st.slider('Length of measured correlation window:', min_value=5,max_value=100, value=30)

    figure = f.calc_pearson_coefficient(corr_asset_left=tradingpairs[0], corr_asset_right=collection,
                                        webvisu=True, window_length=w_length)


    st.pyplot(figure)
except:
    st.warning("Data not ready yet OR not enough data to calculate")












# try:

#     st.subheader(f"Correlation graph of {tradingpairs[0]} <-> {collection}")
#     st.write("Only takes the first Tradingpair in the selection and compares it to the NFT-Collection")
#     w_length = st.slider('Length of measured correlation window:', min_value=5,max_value=100)

#     df_array = f.calc_pearson_coefficient(corr_asset_left=tradingpairs[0], corr_asset_right=collection,
#                                         webvisu=True, window_length=w_length)

#     df_merged = df_array[0]

#     df_merged["%-Change"]


#     df_corr = df_array[1]               
#     df_corr["timestamp"] = df_corr["timestamp"] / 1000
#     df_corr["timestamp_readable"] = df_corr["timestamp"].apply(datetime.fromtimestamp)
#     st.line_chart(data=df_corr, x="timestamp_readable",y="corr")
# except:
#     st.warning("Data not ready yet...")
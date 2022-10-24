from datetime import datetime, timedelta
import functions as f
import json, sqlite3, logging
import pandas as pd
# pd.options.mode.chained_assignment = None  # default='warn'

# CONST
ALL_COLLECTIONS = ["degods", "okay_bears", "t00bs", "trippin_ape_tribe", "degenerate_ape_academy", 
                    "abc_abracadabra", "galactic_geckos", "cets_on_creck", "shadowy_super_coder_dao", "primates"]

ALL_TRADINGPAIRS = ["BTCUSDT", "SOLUSDT", "ETHUSDT"]



def step_0():
    """
        Get the most popular Solana NFT Collections
    """
    df = f.get_collections()

    f.write_df_to_sql(df=df,table_name="Solana_Collections")


def step_1(webvisu: bool= False, input_collections: list[str] = []):
    """
        Get the raw data from MagicEden (main site endpoint)
    """
    if webvisu == True:
        collections = input_collections
    else:
        collections = ALL_COLLECTIONS

    for collection in collections:
        # get floorprice
        df_floorprice = f.get_floorPrice(collection=collection, resolution="1d")

        # Write DB
        f.write_df_to_sql(df=df_floorprice, table_name=collection)

def step_2(webvisu: bool= False, input_tradingpairs: list[str] = []):
    """
        Get the raw data for a Tradingpair from Binance
    """
    if webvisu == True:
        traidingpairs = input_tradingpairs
    else:
        traidingpairs = ALL_TRADINGPAIRS

    for tp in traidingpairs:
        # get candles
        df_tradingpair_candles = f.get_tradingpair_candles(traidingpair=tp, start_datetime='1.1.2022 01:00:00', resolution="1d")

        # Write DB
        f.write_df_to_sql(df=df_tradingpair_candles, table_name=tp)

def step_3(webvisu: bool= False, input_collections: list[str] = []):
    """
        Prepare data 1: Calc dollar value for collections
    """
    if webvisu == True:
        collections = input_collections
    else:
        collections = ALL_COLLECTIONS

    for collection in collections:
        # calculate the dollar value for all collections (HINT can easily be looped over)
        df = f.calc_dollar_value_of_collection(tradingpair="SOLUSDT", collection=collection)
        # Write DB
        f.write_df_to_sql(df=df, table_name=collection)


def step_4():
    """
        Prepare data 2: Merge all data in one dataframe
    """
    tradingpairs = ["BTCUSDT", "SOLUSDT", "ETHUSDT"]
    collections = ["okay_bears"] # at the moment only one collection!!!!!

    df = f.create_single_table(tradingpairs=tradingpairs, collections=collections)
    # Write DB
    f.write_df_to_sql(df=df, table_name="df_merge")

def step_5():
    """
        calculate first pearson correlation 
    """
    tradingpairs = ["BTCUSDT", "SOLUSDT", "ETHUSDT"]
    collections = ["okay_bears"] # at the moment only one collection!!!!!

    f.calc_pearson_coefficient_matrix(tradingpairs=tradingpairs, collections=collections)

def step_6():
    """
        Calculate the pearson correlation chart.
    """
    f.calc_pearson_coefficient(corr_asset_left="BTCUSDT", corr_asset_right="okay_bears")

def step_7():
    """
        Show a chart of all collections price data
    """
    f.show_line_chart(arr_assets=ALL_COLLECTIONS)

if __name__ == "__main__":
    print("Start programm ", datetime.now())
    # logging.basicConfig(filename='NFT.log', encoding='utf-8', level=logging.INFO)
    # logging.info("Started logging,  Code running..........  %s", datetime.now())


    step_0()
    # step_1()
    # step_2()
    # step_3()
    # step_4()
    # step_5()
    # step_6()
    # step_7()


    print("Finished programm ", datetime.now())
from datetime import datetime, timedelta
import functions as f
import json, sqlite3, logging
import pandas as pd
# pd.options.mode.chained_assignment = None  # default='warn'

def step_1():
    """
        Get the raw data from MagicEden (main site endpoint)

        Could be looped over for multible collections
    """
    all_collections = ["degods"]

    for collection in all_collections:
        # get floorprice
        df_floorprice = f.get_floorPrice(collection=collection, resolution="1d")

        # Write DB
        f.write_df_to_sql(df=df_floorprice, table_name=collection)

def step_2():
    """
        Get the raw data for a Tradingpair from Binance
    """
    all_tradingpairs = ["BTCUSDT", "SOLUSDT"]

    for tp in all_tradingpairs:
        # get candles
        df_tradingpair_candles = f.get_tradingpair_candles(traidingpair=tp, start_datetime='1.1.2022 01:00:00', resolution="1d")

        # Write DB
        f.write_df_to_sql(df=df_tradingpair_candles, table_name=tp)

def step_3():
    """
        Prepare data for correlation comparison
    """
    # Read DB
    df_collection = f.read_df_from_sql(table_name="degods")
    df_traidingpair = f.read_df_from_sql(table_name="SOLUSDT")

    df_updated_with_dollar_price = f.calc_dollar_value_of_collectoin(df_tradingpair=df_traidingpair, df_floorprice=df_collection)

    # Write DB
    f.write_df_to_sql(df=df_updated_with_dollar_price, table_name="degods")

if __name__ == "__main__":
    print("Start programm ", datetime.now())
    # logging.basicConfig(filename='NFT.log', encoding='utf-8', level=logging.INFO)
    # logging.info("Started logging,  Code running..........  %s", datetime.now())

    step_1()
    step_2()
    step_3()



    print("Finished programm ", datetime.now())
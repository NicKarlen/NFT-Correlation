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

    floorprice_json = f.get_floorPrice(collection="degods", resolution="1d")

    # Create a Dataframe from a dictionary
    df = pd.DataFrame(floorprice_json)

    # Write DB
    f.write_df_to_sql(df=df, table_name="degods")

def step_2():
    """
        Get the raw data for a Tradingpair from Binance
    """
    f.get_tradingpair_candles(traidingpair="BTCUSDT", start_datetime='1.1.2022 01:00:00', resolution="1d")

def step_3():
    """
        Prepare data for correlation comparison
    """
    # Read DB
    df = f.read_df_from_sql(table_name="degods")

    f.create_percent_changes(df=df, columnName="cFP")


if __name__ == "__main__":
    print("Start programm ", datetime.now())
    # logging.basicConfig(filename='NFT.log', encoding='utf-8', level=logging.INFO)
    # logging.info("Started logging,  Code running..........  %s", datetime.now())

    # step_1()
    # step_2()
    step_3()



    print("Finished programm ", datetime.now())
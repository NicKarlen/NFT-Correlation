from datetime import datetime, timedelta
import functions as f
import json, sqlite3, logging
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'

def step_1():
    """
        Get the raw data from MagicEden (main site endpoint)

        Could be looped over for multible collections
    """

    floorprice_json = f.get_floorPrice(collection="degods", resolution="1h")

    # Create a Dataframe from a dictionary
    df = pd.DataFrame(floorprice_json)

    # Write the dataframe "degods" to the database
    con = sqlite3.connect('data/database.db')
    df.to_sql(name='degods', con=con, if_exists="replace", index_label="Myidx")
    con.close()

def step_2():
    """
        Get the raw data for a Tradingpair from Binance
    """
    f.get_tradingpair_price(traidingpair="BTCUSDT", start_datetime='1.1.2022 01:00:00')



if __name__ == "__main__":
    print("Start programm ", datetime.now())
    # logging.basicConfig(filename='NFT.log', encoding='utf-8', level=logging.INFO)
    # logging.info("Started logging,  Code running..........  %s", datetime.now())

    # step_1()

    step_2()



    print("Finished programm ", datetime.now())
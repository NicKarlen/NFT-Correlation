from time import sleep
import requests, json, sqlite3
import cloudscraper
from datetime import datetime
import pandas as pd


def read_df_from_sql(table_name: str) -> pd.DataFrame:
    """
        Read a table in the database and return a pandas dataframe
    """
    con = sqlite3.connect('data/database.db')
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
    con.close()

    return df

def write_df_to_sql(df: pd.DataFrame, table_name: str) -> None:
    """
        Write a pandas DataFrame to the Database
    """
    con = sqlite3.connect('data/database.db')
    df.to_sql(name=table_name, con=con, if_exists="replace", index_label="Myidx")
    con.close()


def get_floorPrice(collection: str, resolution: str):
    """
        Function to get the floor price history for a certain collection with a certain resolution.

        respone example:
 
        "cFP": 4.64,            Close FloorPrice
        "cLC": 507,             Close Listed Count
        "cV": 4.5,              Close Volume
        "maxFP": 4.64,          maximaler FloorPrice während der resolution
        "minFP": 4.64,          minimaler ""
        "oFP": 4.64,            Opening FloorPrice
        "oLC": 507,             Opening Listed Count
        "oV": 0,                Opening Volume
        "ts": 1642406400000     Timestamp

    """
    # raise an error if the argument is not supported
    if resolution not in ["1h", "4h"]:
        raise ValueError("resolution: value not supported, must be one of %r." % ["1h", "4h"])


    url = f"""https://stats-mainnet.magiceden.io/collection_stats/getCollectionTimeSeries/{collection}?edge_cache=true&resolution={resolution}&addLastDatum=true"""
    # we use the cloudscraper libary because it handles the request to websites that are protected against misuse.
    # regular request with the python module "requests" does not work 
    scraper = cloudscraper.create_scraper() 
    res = scraper.get(url)

    return json.loads(res.text)


def get_tradingpair_candles(traidingpair: str, start_datetime: str, resolution: str):
    """
        Get the candle data for a specific tradingpair on binance

        Doku: https://binance-docs.github.io/apidocs/spot/en/#kline-candlestick-data 

        https://api.binance.com/api/v3/klines

        response example:
        
        0:  1499040000000,      // Kline open time
        1:  "0.01634790",       // Open price
        2:  "0.80000000",       // High price
        3:  "0.01575800",       // Low price
        4:  "0.01577100",       // Close price
        5:  "148976.11427815",  // Volume
        6:  1499644799999,      // Kline Close time
        7:  "2434.19055334",    // Quote asset volume
        8:  308,                // Number of trades
        9:  "1756.87402397",    // Taker buy base asset volume
        10: "28.46694368",      // Taker buy quote asset volume
        11: "0"                 // Unused field, ignore.
    """
    # raise an error if the argument is not supported
    if resolution not in ["1h", "4h", "1d"]:
        raise ValueError("resolution: value not supported, must be one of %r." % ["1h", "4h", "1d"])

    # create the unix for the start datetime given
    starttime = int(datetime.strptime(start_datetime, '%d.%m.%Y %H:%M:%S').timestamp() * 1000)

    # calculate the endtime that we always get the 1h-candles for 40d
    time_intervalls = {
        "1h": 3456000000,   # 40d      40d * 24h = 960 Datapoints (Limit is 1000)
        "4h": 13824000000,  # 160d     4 * that of 1h
        "1d": 82944000000   # 960d     960 Datapoints   86400000=1d -> * 960 
    }
    time_offset = {
        "1h": 3600000,  #  3600000 = 1h
        "4h": 14400000, #  3600000 * 4 = 14400000 is 4h
        "1d": 86400000  #  3600000 * 24 = 86400000 is 1d
    }
    endTime = starttime + time_intervalls[resolution]  

    url = 'https://api.binance.com/api/v3/klines'
    candles_until_today = []

    # while loop for as long as we don't get an empty loop as response from the api
    while True:
        # specify the parameters for the request
        params = {
        'symbol': traidingpair,
        'interval': resolution,
        'startTime': starttime,
        'endTime': endTime,
        'limit': 1000
        }

        res = requests.get(url, params=params)
        json_res = json.loads(res.text)

        # exit the loop if array is empty
        if json_res == []:
            break
        # add the response to the full array
        candles_until_today.extend(json_res)
        # adjust start and end time for next request
        starttime = endTime + time_offset[resolution]     
        endTime = endTime + time_intervalls[resolution]      

        sleep(0.6)

    # store in file
    with open('data/test.json', 'w') as outfile:
        json.dump(candles_until_today, outfile)


def calc_dollar_value(df_tradingpair: pd.DataFrame, df_floorprice) -> pd.DataFrame:
    """
        Calculate the dollar value from the Floor Price based on the relavant Traidingpair
    """
    pass

def create_percent_changes(df : pd.DataFrame, column_name: str) -> pd.DataFrame:
    """
        :param str columnName: The Name of the column which should be taken to calculate the percentage changes on

        Create a the percentage change from one datapoint to the next
    """
    print(df.head(50))

    df['%-Change'] = df[column_name].pct_change()

    print(df.head(50))


def calc_pearson_coefficient(df: pd.DataFrame) -> pd.DataFrame:
    """
        Calculate the continues r for one Collection to a Tradingpair
    """
    pass


def calc_pearson_coefficient_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """
        HINT: I need one dataframe with all the percentage changes of the assets i want to compare (BTC, SOL, all Collections)
        Calculate the pearson coefficient 
    """
    pass



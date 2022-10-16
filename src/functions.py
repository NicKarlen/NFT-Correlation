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
    df.to_sql(name=table_name, con=con, if_exists="replace")
    con.close()


def get_floorPrice(collection: str, resolution: str) -> pd.DataFrame:
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
    if resolution not in ["1h", "4h", "1d"]:
        raise ValueError("resolution: value not supported, must be one of %r." % ["1h", "4h", "1d"])


    url = f"""https://stats-mainnet.magiceden.io/collection_stats/getCollectionTimeSeries/{collection}?edge_cache=true&resolution={resolution}&addLastDatum=true"""
    # we use the cloudscraper libary because it handles the request to websites that are protected against misuse.
    # regular request with the python module "requests" does not work 
    scraper = cloudscraper.create_scraper() 
    res = scraper.get(url)

    # Create a Dataframe from a dictionary
    df = pd.DataFrame(json.loads(res.text))

    return df


def get_tradingpair_candles(traidingpair: str, start_datetime: str, resolution: str) -> pd.DataFrame:
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

    # Create a Dataframe from a dictionary
    df = pd.DataFrame(candles_until_today)

    df.columns = [
        "Kline open time",
        "Open price",
        "High price",
        "Low price",
        "Close price",
        "Volume",
        "Kline Close time",
        "Quote asset volume",
        "Number of trades",
        "Taker buy base asset volume",
        "Taker buy quote asset volume",
        "Unused field"
    ]

    return df


def calc_dollar_value_of_collection(tradingpair: str, collection: str) -> None:
    """
        Calculate the dollar value from the Floor Price based on the relavant Traidingpair
    """
    # Read DB
    df_collection = read_df_from_sql(table_name=collection)
    df_tradingpair = read_df_from_sql(table_name=tradingpair)
    # Insert new row
    df_floorprice["cFP in Dollar"] = 0
    # check for the timestamp and calculate the closing Floor price in dollar
    def calc_dollar_price(row):
        # if we do not find a matching timestamp we say that the price is 0
        try:
            price = df_tradingpair.loc[df_tradingpair["Kline Close time"] == (row["ts"]-1), "Close price"].values[0]
        except:
            price = 0
        # floor price in X * X/Dollar (tradingpair)
        row["cFP in Dollar"] = row["cFP"] * float(price)
        # return the adjusted row
        return row
    # run above functinon over all rows
    df_floorprice = df_floorprice.apply(calc_dollar_price, axis=1)

    return df_floorprice


def create_single_table(tradingpairs: list[str], collections: list[str]) -> None:
    """
        Create one Dataframe with all the prices of the analysed traingpair-prices and collection-floorprices
    """
    # get an array of all tradingpair dataframes
    df_arr_tradingpairs = []
    for tp in tradingpairs:
        # read from DB
        df = read_df_from_sql(table_name=tp)
        # drop all unneeded columns
        df.drop([
                "Kline open time",
                "Open price",
                "High price",
                "Low price",
                "Volume",
                "Quote asset volume",
                "Number of trades",
                "Taker buy base asset volume",
                "Taker buy quote asset volume",
                "Unused field"
                ], axis=1, inplace=True)
        # adjust timestamp so it is equal to the one in the collection df
        df["Kline Close time"] = df["Kline Close time"] +1 
        # append to array
        df_arr_tradingpairs.append(df)

    # merge all the traidingpair dataframes on the timestamp
    df_merged = df_arr_tradingpairs[0]
    if len(df_arr_tradingpairs) > 1:
        for df in df_arr_tradingpairs[1:]:
            df_merged = pd.concat([df_merged.set_index('Kline Close time'),df.set_index('Kline Close time')], axis=1, join='inner').reset_index()

    

    # get an array of all collection dataframes
    df_arr_collections = []
    for collection in collections:
        # read from DB
        df = read_df_from_sql(table_name=collection)
        # drop all unneeded columns
        df.drop(["index","cFP","cLC","cV","maxFP","minFP","oFP","oLC","oV"], axis=1, inplace=True)
        df.set_index("level_0", inplace=True)
        # append to dataframe
        df_arr_collections.append(df)

    # DOESN'T WORK AT THE MOMENT WITH MORE THAN ONE COLLECTION!
    # The problem is i would need to make a left join because the collections data is not always the same but then i will have problems
    # making a pearson correlation matrix because i dont compare same timeframes... 
    for df in df_arr_collections:
        df_merged = pd.concat([df_merged.set_index('Kline Close time'),df.set_index('ts')], axis=1, join='inner').reset_index()
   
    # drop all index rows.
    df_merged.drop("index", axis=1, inplace=True)
    # rename the dataframe columns accordingly
    column_names = ["timestamp"]
    column_names.extend(tradingpairs)
    column_names.extend(collections)
    df_merged.columns = column_names
    
    print(df_merged)


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



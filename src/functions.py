from time import sleep
import requests, json, sqlite3
import cloudscraper
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


def read_df_from_sql(table_name: str, special_DB_name : str = "database", is_collection: bool = False) -> pd.DataFrame:
    """
        Read a table in the database and return a pandas dataframe
    """
    # change table name because there are some collection name that start with numbers
    if is_collection == True:
        table_name = f"nft_{table_name}"

    con = sqlite3.connect(f'data/{special_DB_name}.db')
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", con)
    con.close()

    return df

def write_df_to_sql(df: pd.DataFrame, table_name: str, special_DB_name : str = "database", is_collection: bool = False) -> None:
    """
        Write a pandas DataFrame to the Database
    """
    # change table name because there are some collection name that start with numbers
    if is_collection == True:
        table_name = f"nft_{table_name}"

    con = sqlite3.connect(f'data/{special_DB_name}.db')
    df.to_sql(name=table_name, con=con, if_exists="replace")
    con.close()

def get_all_collection_names_from_DB():
    # Making a connection
    sqliteConnection = sqlite3.connect('data/database.db')
    # Getting all tables from sqlite_master
    sql_query = "SELECT name FROM sqlite_master WHERE type='table';"
    # Creating cursor object using connection object
    cursor = sqliteConnection.cursor()
    # executing our sql query
    cursor.execute(sql_query)
    # printing all tables list
    tuple_db_table = cursor.fetchall()
    arr_saved_collections = []
    for table in tuple_db_table:
        if table[0][:3] == "nft":
            arr_saved_collections.append(table[0][4:])

    arr_saved_collections.sort()
    return arr_saved_collections

def get_collections() -> pd.DataFrame:
    """
        Function to get the top 1000 most popular NFT collections from MagicEden
    """

    url = "https://stats-mainnet.magiceden.io/collection_stats/popular_collections/sol?limit=1000&window=30d"

    # we use the cloudscraper libary because it handles the request to websites that are protected against misuse.
    # regular request with the python module "requests" does not work
    scraper = cloudscraper.create_scraper()
    res = scraper.get(url)

    # Create a Dataframe from a dictionary
    df = pd.DataFrame(json.loads(res.text))

    return df

def get_arr_collection_names():
    """
        Return an array with all the collection-names from the top 1000 most popular collections from MagicEden
    """
    # get all the collections from the DB, order them by total Volume and create an array of collection names
    df_top_collections = read_df_from_sql(table_name="Solana_Collections")
    df_top_collections.sort_values(by="totalVol", ascending=False,inplace=True)
    arr_top_collection = df_top_collections["collectionSymbol"].values

    return arr_top_collection



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
    sleep(0.3)
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
    # name the columns of the dataframe
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


def calc_dollar_value_of_collection(tradingpair: str, collection: str = None, df_direct : pd.DataFrame = pd.DataFrame) -> pd.DataFrame:
    """
        Calculate the dollar value from the Floor Price based on the relavant Traidingpair
    """
    # Read DB
    if df_direct.empty:
        df_collection = read_df_from_sql(table_name=collection, is_collection=True)
    else:
        df_collection = df_direct

    df_tradingpair = read_df_from_sql(table_name=tradingpair)
    # Insert new row
    df_collection["cFP in Dollar"] = 0
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
    df_collection = df_collection.apply(calc_dollar_price, axis=1)

    return df_collection


def create_single_table(tradingpairs: list[str], collections: list[str]) -> pd.DataFrame:
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
        df = read_df_from_sql(table_name=collection, is_collection=True)
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

    # change all values to numerics
    df_merged[column_names[1:]] = df_merged[column_names[1:]].apply(pd.to_numeric)

    return df_merged


def calc_pearson_coefficient_matrix(tradingpairs: list[str], collections: list[str]) -> pd.DataFrame:
    """
        Calculate the pearson coefficient matirx

        HINT: I need one dataframe  of the assets i want to compare (BTC, SOL, all Collections)
    """

    select = tradingpairs + collections
    # read DB
    df = read_df_from_sql(table_name="df_merge")
    # select the needed columns
    df_selection = df[select]
    # calculate the pearson correlation coefficients (matix)
    corr = df_selection.corr(method="pearson")

    # print("\nPearson correlation index: \n")
    # print(corr)

    return corr



def calc_pearson_coefficient(corr_asset_left: str, corr_asset_right: str, webvisu: bool= False, window_length: int=30):
    """
        Calculate the continues r for two assets
    """
    # read DB
    df = read_df_from_sql(table_name="df_merge")
    # select the needed columns
    df_selection = df[[corr_asset_left, corr_asset_right]]
    # calculate the pearson correlation coefficients (matix)

    arr_corr = []
    more_values = True
    inc = 0
    length = window_length

    while more_values:

        try:
            corr = df_selection.iloc[inc:length+inc].corr(method="pearson")
            arr_corr.append({
                    "timestamp": df.loc[length+inc, "timestamp"],
                    "corr": corr.iloc[1,0]
                })

            inc = inc + 1
        except:
            more_values = False

    # Create a Dataframe from a dictionary
    df_arr_corr = pd.DataFrame(arr_corr)

    # if webvisu == True:
    #     return [df, df_arr_corr]

    # Create a plot-window with 6 seperate plots with no shared X axis
    fig, axes = plt.subplots(nrows=2, ncols=1, sharex=True)

    df_arr_corr["timestamp"] = df_arr_corr["timestamp"] / 1000
    df_arr_corr["timestamp"] = df_arr_corr["timestamp"].apply(datetime.fromtimestamp)
    df["timestamp"] = df["timestamp"] / 1000
    df["timestamp"] = df["timestamp"].apply(datetime.fromtimestamp)

    df_arr_corr.plot(kind='line', x="timestamp", y="corr",ax=axes[0], xlabel='timestamp', color='black', legend=True)

    df.plot(kind='line', x="timestamp", y=[corr_asset_left,corr_asset_right],
            secondary_y=[corr_asset_right],ax=axes[1], xlabel='timestamp', color=['red',"blue"], legend=True)

    # Show the plot
    if webvisu == True:
        return fig
    plt.show()


def show_line_chart(amount: int) -> None:
    """
        A function that shows a line chart with all the assets

        step0: We get the data form the DB for the asset.
        step1: We calc the percentage change of the Closing FloorPrice in Dollars.
        step2: We make a moving avarage of 25 days
        step3: We create the line
        step4: We show the legend and show the chart

    """
    # get array of collection names sorted by volume
    arr_top_collection = get_arr_collection_names()


    for idx, asset in enumerate(arr_top_collection[:amount]):
        df = get_floorPrice(collection=asset, resolution="1d")
        df = calc_dollar_value_of_collection(tradingpair="SOLUSDT",  df_direct=df)

        df.drop(df.tail(1).index,inplace=True) # drop last n rows
        df.drop(df.head(1).index,inplace=True) # drop first n rows

        df["PercChanges"] = (df["cFP in Dollar"] - df.iloc[0, 9]) / df.iloc[0, 9] * 100

        df[asset] = df["PercChanges"]
        #df[asset] = df["cFP in Dollar"]

        plt.plot("ts", asset, data=df)


    plt.yscale("symlog", linthresh=50)
    # show legend
    plt.legend()
    # Show the plot
    plt.show()


def calc_returns(collection: str, traidingpairs: list[str], delay: int = 0, return_dict: bool = False) -> pd.DataFrame:
    """
        We calculate the ROI of a Collection and compare it to the ROI of a number of traidingpairs (e.g. BTC, SOL, ETH)

        ROI = (Current value of Investment - Cost of Investment) / Cost of Investment

        delay: To delay to start of the measuret (in days after the mint)
    """
    # If the collection exists in the database will take that, else we fetch it via API
    try:
        df_collection = read_df_from_sql(table_name=collection, is_collection=True)
    except:
        df_collection = get_floorPrice(collection=collection, resolution="1d")
        df_collection = calc_dollar_value_of_collection(tradingpair="SOLUSDT",  df_direct=df_collection)

    # Drop the first and last row of the collection data, because the first and last row are no full days
    # and we can therefore not find the timestamp in the tradingpairs data
    df_collection.drop(df_collection.tail(1).index,inplace=True) # drop last n rows
    df_collection.drop(df_collection.head(1+delay).index,inplace=True) # drop first n rows
    # Get first and last row of the adjusted dataframe
    collection_first_row = df_collection.iloc[0]
    collection_last_row = df_collection.iloc[-1]
    # Calc the ROI for the collection from first day to last day
    roi_collection = (collection_last_row["cFP in Dollar"] - collection_first_row["cFP in Dollar"]) / collection_first_row["cFP in Dollar"] * 100

    dict_roi_tp = {}
    # Calc the return of the traidingpairs with the same start date as for the collection
    for tp in traidingpairs:
        df_tp = read_df_from_sql(table_name=tp)
        # search for the row with the same timestamp as the one from the collection
        tp_first_row = df_tp[df_tp["Kline Close time"] == collection_first_row["ts"]-1 ].reset_index()
        tp_last_row = df_tp[df_tp["Kline Close time"] == collection_last_row["ts"]-1 ].reset_index()

        # If we get data from the API that makes problems we just print out the collection name for later investigations
        try:
            dict_roi_tp[tp] = (float(tp_last_row.loc[0,"Close price"]) - float(tp_first_row.loc[0,"Close price"])) / float(tp_first_row.loc[0,"Close price"]) * 100
        except:
            print(tp_first_row)
            print(tp_last_row)

    # if the function is used for the purpose of collecting data for the DB we create the needed dict with all the information
    if return_dict == True:
        dict_roi_tp["collection_ROI"] = roi_collection
        dict_roi_tp["Compare"] = dict_roi_tp["collection_ROI"] - dict_roi_tp[traidingpairs[0]]
        dict_roi_tp["collection"] = collection
        dict_roi_tp["start_timestamp"] = collection_first_row["ts"]
        return dict_roi_tp

    dict_roi_tp[collection] = roi_collection

    # Calc the performance difference to the traindingpairs
    for asset in dict_roi_tp:
        dict_roi_tp[asset] = [dict_roi_tp[asset], dict_roi_tp[collection] - dict_roi_tp[asset]]
    # Create a Dataframe from the Dict
    df_roi = pd.DataFrame.from_dict(dict_roi_tp,orient='index',columns=["ROI", "performance against collection"])

    return df_roi


def prep_compare_all_returns() -> None:
    """
        get all the ROI since beginning of every collection

        RUNTIME: ~10min
    """
    # get array of collection names sorted by volume
    arr_top_collection = get_all_collection_names_from_DB()

    # For every collection we run the calc_returns function and create a new dictionary
    list_all_returns = []
    for idx, coll in enumerate(arr_top_collection):
        try:
            list_all_returns.append(calc_returns(collection=coll, traidingpairs=["BTCUSDT"], return_dict=True))
        except:
            print(coll)

    # Create a Dataframe from the collected data
    df_all_returns = pd.DataFrame.from_dict(list_all_returns)
    # set index to the collection name
    df_all_returns.set_index("collection", inplace=True)
    # save in DB
    write_df_to_sql(df=df_all_returns, table_name="Collection_all_returns")

def plot_compare_all_returns() -> None:
    """
        Function to categorize the ROI's of all collection and make a distribution plot (bar-chart)
    """
    # Read data from DB
    df_returns = read_df_from_sql(table_name="Collection_all_returns")
    # drop null values
    df_returns.dropna(inplace=True)

    # Create the bins and labels for the .cut function.
    # This is a categorization of the data
    arr_bins    = [-100,-90,-80,-70,-60,-50,-40,-30,-20,-10,0,10,20,30,40,50,60,70,80,90,100,200,300,400,500,1000,2000,3000,4000,5000,10000,np.inf]
    arr_labels  = [-90,-80,-70,-60,-50,-40,-30,-20,-10,0,10,20,30,40,50,60,70,80,90,100,200,300,400,500,1000,2000,3000,4000,5000,10000,"inf"]
    arr_labels = [str(label) for label in arr_labels]

    # Perform the categorization for both the collection_ROI and also the "ROI compared to Bitcoin"
    for col in ["collection_ROI", "Compare"]:
        df_returns[col] = df_returns[col].round(decimals=0)
        df_returns[f"{col}_groupe"] = pd.cut(
                                x=df_returns[col],
                                bins=arr_bins,
                                labels=arr_labels,
                            )
    # Check how many values in which category and sort them by the category names
    distribution_collection_ROI = df_returns["collection_ROI_groupe"].value_counts().sort_index()
    distribution_Compare = df_returns["Compare_groupe"].value_counts().sort_index()

    # plot two bar charts
    fig, axes = plt.subplots(nrows=2, ncols=1, sharex=True)
    distribution_collection_ROI.plot.bar(ax=axes[0],ylabel='Count of Collections', xlabel="ROI % Groups")
    distribution_Compare.plot.bar(ax=axes[1],ylabel='Count of Collections', xlabel="ROI % Groups")
    fig.suptitle("Oben der ROI von den Kollektionen\nUnten der ROI im Vergleich zu Bitcoin (über die jeweilige Lebensdauer der Kollektion)")

    plt.show()


def get_traidingdays_per_nft():

    arr_all_collections = get_all_collection_names_from_DB()
    dict_traidingdays = {}
    for idx, coll in enumerate(arr_all_collections):
        try:
            df = read_df_from_sql(table_name=coll, is_collection=True)
        except Exception as e:
            print(e)
        dict_traidingdays[coll] = {"number_of_tradingdays": df.shape[0],
                                    "more_than_50": df.shape[0] > 50,
                                    "more_than_100": df.shape[0] > 100,
                                    "more_than_150": df.shape[0] > 150,
                                    "more_than_200": df.shape[0] > 200,
                                    "more_than_250": df.shape[0] > 250}

    with open('data/length_nft.json', 'w', encoding='utf-8') as f:
        json.dump(dict_traidingdays, f, ensure_ascii=False, indent=4)

    sums = {
        "more_than_50": 0,
        "more_than_100": 0,
        "more_than_150": 0,
        "more_than_200": 0,
        "more_than_250": 0,
        "max": 0,
        "min": 99999
    }
    arr_coll_250Plus = []
    for key, value in dict_traidingdays.items():
        if value["more_than_50"] == True:
            sums["more_than_50"] += 1
        if value["more_than_100"] == True:
            sums["more_than_100"] += 1
        if value["more_than_150"] == True:
            sums["more_than_150"] += 1
        if value["more_than_200"] == True:
            sums["more_than_200"] += 1
        if value["more_than_250"] == True:
            sums["more_than_250"] += 1
            arr_coll_250Plus.append(key)
        if value["number_of_tradingdays"] > sums["max"]:
            sums["max"] = value["number_of_tradingdays"]
        if value["number_of_tradingdays"] < sums["min"]:
            sums["min"] = value["number_of_tradingdays"]

    print(sums)

    return arr_coll_250Plus, dict_traidingdays


def calc_daily_returns_for_collections(df_collection: pd.DataFrame ,start_timestamp: int = 0) -> pd.DataFrame:
    """
        Get the daily returns of a collection scaled to a given startingdate. 

        If no date is given we calculate the daily-returns from day one.

        ROI = (Current value of Investment - Cost of Investment) / Cost of Investment
    """
    # get rid of the two columns if they exist
    if "level_0" in df_collection.columns:
        df_collection.drop(columns=["level_0"], inplace=True)
    if "index" in df_collection.columns:
        df_collection.drop(columns=["index"], inplace=True)

    if "perc return" not in df_collection.columns:
        # if the collection was traiding for less than 4 days we cannot make the return calculation
        if df_collection.shape[0] < 4:
            return df_collection
        # Drop first and last row since we do not have a dollar
        # price because the timestamp did not match the dailyclose price when calc the dollarvalue
        df_collection.drop(df_collection.head(1).index,inplace=True) # drop first row
        df_collection.drop(df_collection.tail(1).index,inplace=True) # drop last row


    # read the first price on the first row
    if start_timestamp == 0:
        start_price = df_collection.iloc[0]["cFP in Dollar"]

        # add a row with the % return starting at the first day
        df_collection["perc return"] = 0
        df_collection["perc return"] = (df_collection["cFP in Dollar"] - start_price) / start_price * 100
    else:
        start_price = df_collection.loc[df_collection["ts"] == start_timestamp]["cFP in Dollar"].values[0]
        # add a row with the % return starting at the first day
        df_collection[f"perc return {int(start_timestamp)}"] = 0
        df_collection[f"perc return {int(start_timestamp)}"] = (df_collection["cFP in Dollar"] - start_price) / start_price * 100
        def set_zero(row):
            if row["ts"] < start_timestamp:
                row[f"perc return {int(start_timestamp)}"] = 0
            return row
        df_collection = df_collection.apply(set_zero, axis=1)


    return df_collection


def calc_daily_returns_for_tradingpair(df_traidingpair: pd.DataFrame, start_timestamp: int = 0) -> pd.DataFrame:
    """
        Get the daily returns of a traidingpair scaled to a given startingdate. 

        If no date is given we calculate the daily-returns from day one.
    """
    df_traidingpair["Close price"] = pd.to_numeric(df_traidingpair["Close price"])

    if start_timestamp == 0:
        start_price = df_traidingpair.iloc[0]["Close price"]

        df_traidingpair["perc return"] = 0
        df_traidingpair["perc return"] = (df_traidingpair["Close price"] - start_price) / start_price * 100

    else:
        start_price = df_traidingpair.loc[df_traidingpair["Kline Close time"] == start_timestamp-1]["Close price"].values[0]

        # add a row with the % return starting at the first day
        df_traidingpair[f"perc return {int(start_timestamp)}"] = 0
        df_traidingpair[f"perc return {int(start_timestamp)}"] = (df_traidingpair["Close price"] - start_price) / start_price * 100
        def set_zero(row):
            if row["Kline Close time"] < start_timestamp:
                row[f"perc return {int(start_timestamp)}"] = 0
            return row
        df_traidingpair = df_traidingpair.apply(set_zero, axis=1)

    return df_traidingpair


def create_NFT_Price_Index(collections: list[str], start_timestamp: int = 0):
    """
        function to create a merged dataframe with all collections daily returns and calc a daily simpel avg.

        similar to function "create_single_table"
    """

    for idx, collection in enumerate(collections):
        # read from DB
        df = read_df_from_sql(table_name=collection, is_collection=True)
        # drop all unneeded columns
        df.drop(["index","cFP","cLC","cV","maxFP","minFP","oFP","oLC","oV","cFP in Dollar", "perc return"], axis=1, inplace=True)

        if idx == 0:
            df_merged = df
            df_merged.columns = ["ts", collection]
            df_merged = df_merged[df_merged["ts"] >= start_timestamp].reset_index()
            continue
        
        df_merged = pd.concat([df_merged.set_index('ts'),df.set_index('ts')], axis=1, join='inner').reset_index()
        # get rid of the two columns if they exist
        if "level_0" in df_merged.columns:
            df_merged.drop(columns=["level_0"], inplace=True)
        if "index" in df_merged.columns:
            df_merged.drop(columns=["index"], inplace=True)
        df_merged.columns = [*df_merged.columns[:-1], collection]


    names = [*df_merged.columns[1:]]
    df_merged["avg"] = df_merged[names].mean(axis=1)

    print(df_merged)

    return df_merged

def plot_NFT_Price_Index(df_traidingpair: pd.DataFrame) -> None:
    """
        Plot a line chart with the nft price index and a traidingpair (like BTC/USDT)
    """

    df_traidingpair["Kline Close time"] = df_traidingpair["Kline Close time"] +1

    df_nft_price_index = read_df_from_sql(table_name="`NFT Price Index`")

    df_traidingpair = df_traidingpair[df_traidingpair["Kline Close time"] >= df_nft_price_index.loc[0,"ts"]]

    # Correlation
    if "level_0" in df_traidingpair.columns:
        df_traidingpair.drop(columns=["level_0"], inplace=True)
    if "index" in df_traidingpair.columns:
        df_traidingpair.drop(columns=["index"], inplace=True)

    df_traidingpair.reset_index(inplace=True)
    df_nft_price_index.reset_index(inplace=True)
    df = pd.concat([df_nft_price_index["avg"], df_traidingpair[df_traidingpair.columns[-1]]], axis=1, ).reset_index()

    df.drop(df.tail(7).index,inplace=True) # drop last row

    if "index" in df.columns:
        df.drop(columns=["index"], inplace=True)

    correlation = df.corr(method="pearson").iloc[1,0].round(3)

    df_nft_price_index["ts"] = df_nft_price_index["ts"] / 1000
    df_nft_price_index["ts"] = df_nft_price_index["ts"].apply(datetime.fromtimestamp)
    df_traidingpair["Kline Close time"] = df_traidingpair["Kline Close time"] / 1000
    df_traidingpair["Kline Close time"] = df_traidingpair["Kline Close time"].apply(datetime.fromtimestamp)

    fig, axes = plt.subplots(nrows=1, ncols=1, sharex=True)

    df_nft_price_index.plot(kind='line', x="ts", y="avg",ax=axes, xlabel='timestamp', ylabel="Perc Return", legend=True)

    df_traidingpair.plot(kind='line', x="Kline Close time", y=df_traidingpair.columns[-1], ax=axes, xlabel='Timestamp',  legend=True)

    axes.legend(['NFT Price Index', 'BTC/USDT'])
    fig.suptitle(f"NFT Price Index aus 200 Solana NFT-Kollektionen.\nKorrelation liegt bei {correlation}")
    plt.grid(True)
    plt.show()
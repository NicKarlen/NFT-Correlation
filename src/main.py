from datetime import datetime, timedelta
import functions as f
import json, sqlite3, logging
import pandas as pd
# pd.options.mode.chained_assignment = None  # default='warn'

# CONST
ALL_COLLECTIONS = ["degods", "okay_bears", "t00bs", "trippin_ape_tribe", "degenerate_ape_academy", 
                    "abc_abracadabra", "galactic_geckos", "cets_on_creck", "shadowy_super_coder_dao", "primates"]

ALL_TRADINGPAIRS = ["BTCUSDT", "SOLUSDT", "ETHUSDT"]



def step_0(special_DB_name : str = "database"):
    """
        Get the most popular Solana NFT Collections
    """
    df = f.get_collections()

    f.write_df_to_sql(df=df,table_name="Solana_Collections", special_DB_name=special_DB_name)


def step_1(collections: list[str] = ALL_COLLECTIONS, special_DB_name : str = "database"):
    """
        Get the raw data from MagicEden (main site endpoint)
    """
    arr_collection_with_problem = []
    for idx, collection in enumerate(collections):
        try:
            # get floorprice
            df_floorprice = f.get_floorPrice(collection=collection, resolution="1d")

            # Write DB
            f.write_df_to_sql(df=df_floorprice, table_name=collection, special_DB_name=special_DB_name, is_collection=True)
        except Exception as e:
            print(collection)
            print(e)
            arr_collection_with_problem.append(collection)
        print(f"step_1 Nr: {idx}")

    print(arr_collection_with_problem)

def step_2(traidingpairs: list[str] = ALL_TRADINGPAIRS, special_DB_name : str = "database"):
    """
        Get the raw data for a Tradingpair from Binance
    """

    for tp in traidingpairs:
        # get candles
        df_tradingpair_candles = f.get_tradingpair_candles(traidingpair=tp, start_datetime='1.1.2021 01:00:00', resolution="1d")

        # Write DB
        f.write_df_to_sql(df=df_tradingpair_candles, table_name=tp, special_DB_name=special_DB_name)

def step_3(collections: list[str] = ALL_COLLECTIONS):
    """
        Prepare data 1: Calc dollar value for collections
    """
    arr_collection_with_problem = []
    for idx, collection in enumerate(collections):
        try:
            # calculate the dollar value for all collections (HINT can easily be looped over)
            df = f.calc_dollar_value_of_collection(tradingpair="SOLUSDT", collection=collection)
            # Write DB
            f.write_df_to_sql(df=df, table_name=collection, is_collection=True)
        except Exception as e:
            print(collection)
            print(e)
            arr_collection_with_problem.append(collection)
        print(f"step_3: Nr: {idx}")

    print(arr_collection_with_problem)

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
    f.show_line_chart(amount=20)


def step_8():
    """ 
        Compare the returns on Investment. For NFT Collections we assume that one would invested
        on the close of the first (or after a give delay) traidingday.
        For Tradingpairs we take the same date as the NFT Collection.
    """

    #f.calc_returns(collection="metamercs_by_metacreed", traidingpairs=["BTCUSDT", "SOLUSDT"], delay=0)
    f.calc_returns(collection="metamercs_by_metacreed", traidingpairs=ALL_TRADINGPAIRS, delay=0)

def step_9():
    """
        show a chart of the top 1000 collection returns

        Worked for all the 1000 collection except:
            citizens_by_solsteads, synergian, the_lurkers, senseilabs, le_dao, mj98, deadlyroulette, 
            casinonft, cold_sun, winning, imnotwordy, anima_alternis, uyab, trippart, magicstar
    """
    # create data
    # f.prep_compare_all_returns() # -> runs for more than 1min
    # plot data
    f.plot_compare_all_returns()


"""
--------------------------------------------------------------------------
                        NFT-Price-Index (01.11.2022)

"""

def step_10():
    """
        get the collections with more than 320 traidingdays and the starting timestamp
        of the collection with just above 320 tradingdays.
    """  
    arr_coll_320days, dict_tradingdays = f.get_traidingdays_per_nft()
    arr_shape = []
    for i in arr_coll_320days:
        arr_shape.append(dict_tradingdays[i]["number_of_tradingdays"])

    arr_shape.sort()

    for key, value in dict_tradingdays.items():
        if value["number_of_tradingdays"] == arr_shape[0]:
            df = f.read_df_from_sql(table_name=key, is_collection=True)
            starting_timestamp = df.loc[1]["ts"]
            break

    print(starting_timestamp)

    return starting_timestamp, arr_coll_320days

def step_11(input_collections: list[str] = None, start_timestamp: int = 0):
    """
        calc daily returns for all collections starting at the beginning or the start_timestamp.
    """
    if input_collections == None:
        arr_collections = f.get_all_collection_names_from_DB()
    else:
        arr_collections = input_collections

    for idx, coll in enumerate(arr_collections):
        print(idx, coll)
        df_coll = f.read_df_from_sql(table_name=coll, is_collection=True)
        if start_timestamp == 0:
            df_coll = f.calc_daily_returns_for_collections(df_collection=df_coll)
        else:
            df_coll = f.calc_daily_returns_for_collections(df_collection=df_coll, start_timestamp=start_timestamp)
        f.write_df_to_sql(df=df_coll,table_name=coll, is_collection=True)


def step_12(start_timestamp: int = 0):
    """
        calc daily returns for all traidingpairs starting at the beginning or the start_timestamp.
    """

    df_tp = f.read_df_from_sql(table_name="BTCUSDT")
    df_tp = f.calc_daily_returns_for_tradingpair(df_traidingpair=df_tp,start_timestamp=start_timestamp)
    f.write_df_to_sql(df=df_tp,table_name="BTCUSDT")
    
def step_13(input_collections: list[str] = ["degods", "galactic_geckos"], start_timestamp: int = 0):
    """
        creat one dataframe with the collections that are older than 250 days and the to be compared traidingpair.
    """
    df_nft_price_index = f.create_NFT_Price_Index(collections = input_collections, start_timestamp = start_timestamp)

    f.write_df_to_sql(df=df_nft_price_index, table_name="NFT Price Index")


def step_14():
    """
        plot NFT-Price-Index and Bitcoin chart
    """
    df_btc = f.read_df_from_sql(table_name="BTCUSDT")
    f.plot_NFT_Price_Index(df_traidingpair=df_btc)



if __name__ == "__main__":
    print("Start programm ", datetime.now())
    # logging.basicConfig(filename='NFT.log', encoding='utf-8', level=logging.INFO)
    # logging.info("Started logging,  Code running..........  %s", datetime.now())


    # step_0()
    # step_1()
    # step_2()
    # step_3()
    # step_4()
    # step_5()
    # step_6()
    # step_7()
    # step_8()
    # step_9()
    """
        Delete DB before running it...
    """
    st, colls = step_10()
    step_11(input_collections=colls, start_timestamp=st)
    step_12(start_timestamp=st)
    step_13(input_collections = colls, start_timestamp=st)
    step_14()

    """
        Collect raw data for all collections and tradingpairs (new DB)
    """
    # step_0(special_DB_name = "database_all_collections")
    # df_top_collections = f.read_df_from_sql(table_name="Solana_Collections", special_DB_name = "database_all_collections")
    # df_top_collections.sort_values(by="totalVol", ascending=False,inplace=True)
    # arr_top_collection = df_top_collections["collectionSymbol"].values
    # step_1(webvisu = True, input_collections=arr_top_collection, special_DB_name = "database_all_collections")
    # step_2(special_DB_name = "database_all_collections")
    """
        Collect raw data
        HINT: DELETE DB BEFORE RUNNING!!!
    """
    # step_0()
    # step_2()

    # # get array of collection names from newly downloaded collections
    # df_top_collections = f.read_df_from_sql(table_name="Solana_Collections")
    # df_top_collections.sort_values(by="totalVol", ascending=False,inplace=True)
    # arr_top_collection = df_top_collections["collectionSymbol"].values

    # step_1(collections=arr_top_collection)

    # # get array of collection names sorted by volume FROM DB
    # arr_top_collection = f.get_all_collection_names_from_DB()

    # step_3(collections=arr_top_collection)

    """
        test
    """

    print("Finished programm ", datetime.now())
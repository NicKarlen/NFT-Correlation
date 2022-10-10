from datetime import datetime, timedelta
import functions as f
import json, sqlite3, logging
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'

def step_1():
    """
        Get the raw data from MagicEden via API
    """
    activities = f.get_activities("degods")

    # Create or open an existing file and write a dict in the file
    with open('data/activities_no_filter.json', 'w') as outfile:
        json.dump(activities, outfile)


def step_2():
    """
        Read json from file, safe it in a dataframe, filter dublicates and safe it to DB
    """
    # Opening JSON file
    f = open('data/activities_no_filter.json')  # activities_everything
    # returns JSON object as a dictionary
    data = json.load(f)
    # Create a Dataframe from a dictionary
    df = pd.DataFrame(data)
    # Drop dublicates that were caused by the way the API is structured.
    # df.drop_duplicates(inplace=True)

    # Write the dataframe "degods" to the database
    con = sqlite3.connect('data/database.db')
    df.to_sql(name='degods', con=con, if_exists="replace", index_label="Myidx")
    con.close()

    #step_3(df)



def step_3(df: pd.DataFrame):
    """
        Creating a table with the price start- and end-Blocktime.
        1. Get all unique tokenMints
        2. For each tokenMint-adress go throw the activities table and
        write down the prices with their starting and end date
    """
    # initialize a list for all active listings
    all_active_Listings = []

    # Function that is run on every single row.
    def check_each_row(row):
        
        if row["type"] == "list":

            if active_Listings:
                if active_Listings[-1]["endType"] == 0:
                    if active_Listings[-1]["seller"] != row["seller"]: # Special case where some entries were missing. Now we check if the new seller is equal the old seller. If not we assume that a entry ("sell") is missing.
                        active_Listings[-1]["endType"] = "missingSell"
                        active_Listings[-1]["endTime"] = active_Listings[-1]["startTime"]+1000
                    else:
                        active_Listings[-1]["endType"] = "relist"
                        active_Listings[-1]["endTime"] = row["blockTime"]
            

            active_Listings.append({
                            "tokenMint": row["tokenMint"],
                            "startType": row["type"],
                            "startTime": row["blockTime"],
                            "endType" : 0,
                            "endTime": 0,
                            "floorPrice" : row["price"],
                            "seller": row["seller"]
                            })


        if row["type"] in ["delist", "buyNow", "buy"]:
            if active_Listings:
                if active_Listings[-1]["endTime"] == 0: # Only write the endTime if it is not 0 (data is not perfect: sometimes a delist and much later a BuyNow)
                    active_Listings[-1]["endType"] = row["type"]
                    active_Listings[-1]["endTime"] = row["blockTime"]


    # get all unique tokenMint adresses
    tokenAdresses = df["tokenMint"].unique()
    # Current FloorPrice of Collection
    current_floorPrice_of_collection = f.get_current_FloorPrice("degods")

    # for loop over all adresses
    for index, tokenAdr in enumerate(tokenAdresses):
        # get a table with all entries from a certian adress
        df_tokenAdr = df.loc[df['tokenMint'] == tokenAdr] 
        # order the dataframe by blocktime in ascending order
        df_tokenAdr.sort_index(ascending=False, inplace=True)      #df_tokenAdr.sort_values(by=["blockTime"], ascending=True, inplace=True)  Wrong because because of sniper bots that trigger a buy in the same block...

        # run the funtion on each row
        active_Listings = []
        df_tokenAdr.apply(check_each_row, axis=1)

        # Handle tokenAdresses that have no finally buyNow or delist event. We compare how high the price is compared to the
        # current floorPrice and if it is below we "ignore" it by just adding a endTime a little higher than the start time.
        # If the price is higher than the floor price we assume that it is actually still an open listing and don't change it!
        try:
            if active_Listings[-1]["endTime"] == 0 and active_Listings[-1]["floorPrice"] < current_floorPrice_of_collection:
                active_Listings[-1]["endTime"] = active_Listings[-1]["startTime"]+1000
        except:
            #logging.info("This is an Adress that has no listing: %s", tokenAdr)
            pass

        all_active_Listings.extend(active_Listings)


    # Create a Dataframe from a dictionary
    df = pd.DataFrame(all_active_Listings)

    # Write the dataframe "degods" to the database
    con = sqlite3.connect('data/database.db')
    df.to_sql(name='degods_active_listings', con=con, if_exists="replace")
    con.close()


def step_4():
    """
        Build a continous floorprice based on the active Listings.
    """

    con = sqlite3.connect('data/database.db')
    df = pd.read_sql_query("SELECT * FROM degods_active_listings", con)
    con.close()


    # Array to store every change in floorprice with the parameters tokenMint
    floorprice_table = [ {
        "tokenMint": "xxx",
        "startType": "list",
        "startTime": 0,
        "endType" : "list",
        "endTime": 0,
        "floorPrice" : 99999
    }]

    # Sort the dataframe by blocktime in ascending order
    df.sort_values(by=["startTime"], ascending=True, inplace=True)

    # Function that is run on every single row.
    def check_lowest_list(row):

        if row["floorPrice"] < floorprice_table[-1]['floorPrice']:

            floorprice_table.append({
                "tokenMint": row["tokenMint"],
                "startType": row["startType"],
                "startTime": row["startTime"],
                "endType" : row["endType"],
                "endTime": row["endTime"],
                "floorPrice" : row["floorPrice"]
            })

        if floorprice_table[-1]['endTime'] < row["startTime"] and floorprice_table[-1]['endTime'] != 0:

            df_timewindow = df.loc[(df['startTime'] <= row["startTime"]) & (df['endTime'] > row["startTime"]) ]

            df_timewindow.sort_values(by=["floorPrice"], ascending=True, inplace=True)

            floorprice_table.append({
                "tokenMint": df_timewindow["tokenMint"].iloc[0],
                "startType": df_timewindow["startType"].iloc[0],
                "startTime": df_timewindow["startTime"].iloc[0],
                "endType" : df_timewindow["endType"].iloc[0],
                "endTime": df_timewindow["endTime"].iloc[0],
                "floorPrice" : df_timewindow["floorPrice"].iloc[0]
            })
        
        


    df.apply(check_lowest_list, axis=1)
    
    
    # Create a Dataframe from a dictionary
    df = pd.DataFrame(floorprice_table[1:])

    con = sqlite3.connect('data/database.db')
    df.to_sql(name='degods_floorPrice', con=con, if_exists="replace")
    con.close()
    



if __name__ == "__main__":
    print("Start programm ", datetime.now())
    # logging.basicConfig(filename='NFT.log', encoding='utf-8', level=logging.INFO)
    # logging.info("Started logging,  Code running..........  %s", datetime.now())

    # step_1()

    step_2() # with step_3()

    #step_4()


    print("Finished programm ", datetime.now())
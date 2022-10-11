from time import sleep
import requests, json
import cloudscraper
from datetime import datetime



def get_floorPrice(collection: str, resolution: str):
    """
        Function to get the floor price history for a certain collection with a certain resolution.

        respone example:
 
        "cFP": 4.64,            Current FloorPrice
        "cLC": 507,             Current Listed Count
        "cV": 4.5,              Current Volume
        "maxFP": 4.64,          maximaler FloorPrice während der resolution
        "minFP": 4.64,          minimaler ""
        "oFP": 4.64,            Opening FloorPrice
        "oLC": 507,             Opening Listed Count
        "oV": 0,                Opening Volume
        "ts": 1642406400000     Timestamp

    """

    url = f"""https://stats-mainnet.magiceden.io/collection_stats/getCollectionTimeSeries/{collection}?edge_cache=true&resolution={resolution}&addLastDatum=true"""

    # we use the cloudscraper libary because it handles the request to websites that are protected against misuse.
    # regular request with the python module "requests" does not work 
    scraper = cloudscraper.create_scraper() 
    res = scraper.get(url)
    
    return json.loads(res.text)


def get_tradingpair_price(traidingpair: str, start_datetime : str):
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

    # create the unix for the start datetime given
    starttime = int(datetime.strptime(start_datetime, '%d.%m.%Y %H:%M:%S').timestamp() * 1000)
    # calculate the endtime that we always get the 1h-candles for 40d
    endTime = starttime + 3456000000  # 40d = 3456000000
    # assign the url for the endpoint
    url = 'https://api.binance.com/api/v3/klines'
    # creat the array to store all the responses
    candles_until_today = []
    # while loop for as long as we don't get an empty loop as response from the api
    while True:
        # specify the parameters for the request
        params = {
        'symbol': traidingpair,
        'interval': "1h",
        'startTime': starttime,
        'endTime': endTime,
        'limit': 1000
        }
        # make the request
        res = requests.get(url, params=params)
        # parse the response in to a json obj
        json_res = json.loads(res.text)
        # exit the loop if array is empty
        if json_res == []:
            break
        # add the response to the full array
        candles_until_today.extend(json_res)
        # adjust start and end time for next request
        starttime = endTime + 3600000           # 1h = 3600000
        endTime = endTime + 3456000000    
        # timeout so we don't trigger the rates limit of the api
        sleep(0.6)

    # store in file
    with open('data/test.json', 'w') as outfile:
        json.dump(candles_until_today, outfile)
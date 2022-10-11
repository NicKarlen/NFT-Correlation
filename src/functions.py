import requests, json
import cloudscraper



def get_floorPrice(collection: str, resolution: str):
    """
        Function to get the floor price history for a certain collection with a certain resolution.    
    """

    url = f"""https://stats-mainnet.magiceden.io/collection_stats/getCollectionTimeSeries/{collection}?edge_cache=true&resolution={resolution}&addLastDatum=true"""

    # we use the cloudscraper libary because it handles the request to websites that are protected against misuse.
    # regular request with the python module "requests" does not work 
    scraper = cloudscraper.create_scraper() 

    req = scraper.get(url)
    return json.loads(req.text)

def get_tradingpair_price(traidingpair: str, resolution: str):
    

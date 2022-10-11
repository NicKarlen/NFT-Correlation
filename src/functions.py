from time import sleep
import requests, json

"""
    Fetch api form MagicEden and get the raw data for all the activities
    The types are : [bid
                    cancelBid
                    delist
                    list
                    buyNow
                    auctionCreated
                    auctionSettled
                    auctionPlaceBid
                    auctionCanceled
                    buy]
    I will ignore the actions
"""
# 
def get_activities(collection: str):

    def do_request(collection: str, offset: int):
        url = f"https://api-mainnet.magiceden.dev/v2/collections/{collection}/activities?offset={offset}&limit=1000"
        req = requests.get(url)

        return json.loads(req.text)


    # Loop through all activities that we can fetch, stop when the returned value is an empty array
    response_empty = False
    offset = 0

    activities =[]
    while not response_empty:
        json_res = do_request(collection, offset)
        # Check if the response is empty
        if json_res == []:
            response_empty = True
            break
        
        count = 0
        # Filter out the type of transaction that are not need to create a floor price model
        for item in json_res:
            # if item["type"] not in ['list', 'delist', 'buyNow', 'buy']:
            #     continue
            activities.append(item)
            count = count +1
        
        offset = offset + 1000
        print(f"Offset is: {offset}")
        print(f"Count is: {count}")
        count = 0
        sleep(0.6)

    # XXX needs to be interduces: do check for dublicates we can create a "set()" and go back to a "list()"... list(set(XX))
    # or i do it in a next step
    
    return activities

"""
    Get current FloorPrice of a project. (Single value)
"""

def get_current_FloorPrice(collection : str):

    url = f"https://api-mainnet.magiceden.dev/v2/collections/{collection}/stats"
    req = requests.get(url)
    res = json.loads(req.text)

    return res["floorPrice"] / 1000000000
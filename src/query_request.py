import requests, json
import cloudscraper
# attemnt to use the query api from the main site but i don't have access... link: https://devpost.com/software/analytics-seller-classification 

def get_query_test():

    tokenAdr = "6sRi2djxh5uuV2Z3r8eTmVVJSQZJ3yvT2tnijXXg5pGo"
    # wichtig -> Abfrage für floorPrice daily
    url = f"""https://stats-mainnet.magiceden.io/collection_stats/getCollectionTimeSeries/degods?edge_cache=true&resolution=1d&addLastDatum=true"""
    
    scraper = cloudscraper.create_scraper() 

    req = scraper.get(url)
    #res = json.loads(req.text)

    print(req.text)


if __name__ == "__main__":

    get_query_test()
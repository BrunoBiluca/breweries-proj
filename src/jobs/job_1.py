from pyspark.sql import SparkSession
import requests
import json

def start():
    spark = SparkSession.builder \
        .appName("RetrieveBreweriesJob") \
        .getOrCreate()

    url = "https://api.openbrewerydb.org/v1/breweries"

    try:
        res = requests.get(url)
        
    except Exception as e:
        print(e)
        
    if res != None and res.status_code == 200:
        print(json.loads(res.text))
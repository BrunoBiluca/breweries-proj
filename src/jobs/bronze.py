from pyspark.sql import SparkSession
from common.rest_api_caller import get


def start():
    spark: SparkSession = (SparkSession.builder
                           .appName("BronzeBreweriesJob")
                           .getOrCreate())

    url = "https://api.openbrewerydb.org/v1/breweries?page=1&per_page=200"

    try:
        res = get(url)

    except Exception as e:
        print(e)

    df = spark.createDataFrame(res)

    (df
     .write
     .mode("overwrite")
     .saveAsTable("breweries_bronze"))

import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, schema_of_json, from_json
from pyspark.sql.types import StringType, Row


def get(url):
    headers = {
        'Content-Type': "application/json",
        'User-Agent': "apache spark 3.x"
    }
    res = None
    try:
        res = requests.get(url, headers=headers)
    except Exception as e:
        return e
    if res != None and res.status_code == 200:
        return res.text
    return None


def get_total_breweries():
    meta = get("https://api.openbrewerydb.org/v1/breweries/meta")
    meta_json = json.loads(meta)
    print("Total breweries: " + str(meta_json["total"]))
    return meta_json["total"]


spark: SparkSession = (SparkSession.builder
                        .appName("BronzeBreweriesJob")
                        .getOrCreate())

reqs = []
rest_api_url = Row("url")
per_page = 200
total_pages = int(get_total_breweries() / per_page)
for pageIdx in range(1, total_pages):
    reqs.append(rest_api_url(f"https://api.openbrewerydb.org/v1/breweries?page={pageIdx}&per_page={per_page}"))

df = spark.createDataFrame(reqs)

udf_getRestApi = udf(get, StringType())
df = df.withColumn("result", udf_getRestApi(col("url")))

json_schema = schema_of_json(df.select("result").first()[0])
df = (df.withColumn("result_json", from_json(col("result"), json_schema))
        .selectExpr("explode(result_json) as brewery")
        .selectExpr("brewery.*"))

(df
    .write
    .mode("overwrite")
    .option("mergeSchema", True)
    .save("spark-warehouse/breweries_bronze"))

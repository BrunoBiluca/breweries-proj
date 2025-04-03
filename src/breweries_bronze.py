import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, schema_of_json, from_json
from pyspark.sql.types import StringType, Row, StructType, StructField, IntegerType


def get(url):
    headers = {
        'Content-Type': "application/json",
        'User-Agent': "apache spark 3.x"
    }
    try:
        res = requests.get(url, headers=headers)
        return (res.status_code, res.text)
    except Exception as e:
        return e


def get_total_breweries():
    meta = get("https://api.openbrewerydb.org/v1/breweries/meta")
    
    if meta[0] != 200:
        raise Exception("Failed to get breweries meta with code: " + str(meta[0]) + "\n" + meta[1])
    
    try:
        meta_json = json.loads(meta[1])
        total = meta_json["total"]
    except Exception:
        raise Exception("Failed to parse breweries meta: " + meta[1])
    
    print("Total breweries: " + str(total))
    return total


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

udf_getRestApi = udf(get, returnType=StructType([
    StructField("status_code", IntegerType(), True),
    StructField("text", StringType(), True)
]))
df = (df.withColumn("result", udf_getRestApi(col("url")))
      .select(col("url"), col("result.status_code").alias("status_code"), col("result.text").alias("result_text")))

df.cache()

requests_not_ok =  df.filter(col("status_code") != 200).count()
if requests_not_ok > 0:
    raise Exception("Some requests failed: " + str(requests_not_ok))
else:
    print("All requests ok")

json_schema = schema_of_json(df.select("result_text").first()[0])
df = (df.withColumn("result_json", from_json(col("result_text"), json_schema))
        .selectExpr("explode(result_json) as brewery")
        .selectExpr("brewery.*"))

(df
    .write
    .mode("overwrite")
    .option("mergeSchema", True)
    .save("spark-warehouse/breweries_bronze"))

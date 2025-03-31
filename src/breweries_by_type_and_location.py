from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count


spark: SparkSession = (SparkSession.builder
                       .appName("BreweriesByTypeAndLocationJob")
                       .getOrCreate())

(spark
    .read
    .load("spark-warehouse/breweries", format="parquet")
    .groupBy(col("brewery_type"), col("country"))
    .agg(count("*").alias("breweries_count"))
    .write
    .mode("overwrite")
    .save("spark-warehouse/breweries_by_type_and_location_VW"))
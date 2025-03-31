from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim


spark: SparkSession = (SparkSession.builder
                       .appName("BreweriesJob")
                       .getOrCreate())

(spark
    .read
    .load("spark-warehouse/breweries_bronze", format="parquet")
    .withColumn("country", trim(col("country")))
    .repartition(col("country"))
    .write
    .mode("overwrite")
    .partitionBy("country")
    .save("spark-warehouse/breweries"))

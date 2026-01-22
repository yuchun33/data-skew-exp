from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, lit, concat, floor, rand, explode
import pyspark.sql.functions as F

spark = (
    SparkSession.builder.appName("data-skew-turn-off-auto-broadcast-gen")
    # .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1").getOrCreate()
)

df_large = (
    spark.range(0, 100000000)
    .withColumn("join_key", when(col("id") < 80000000, 1).otherwise(col("id") % 10000))
    .withColumn("payload", lit("x" * 100))
)

df_small = spark.range(0, 10000).withColumnRenamed("id", "join_key")

result = df_large.join(df_small, "join_key").count()
